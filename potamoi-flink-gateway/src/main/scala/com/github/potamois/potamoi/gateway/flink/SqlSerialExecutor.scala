package com.github.potamois.potamoi.gateway.flink

import akka.Done
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, PostStop}
import com.github.potamois.potamoi.commons.TryImplicits.Wrapper
import com.github.potamois.potamoi.commons.{CancellableFuture, FiniteQueue, Using, curTs}
import com.github.potamois.potamoi.gateway.flink.SqlSerialExecutor.Command
import com.github.potamois.potamoi.gateway.flink.TrackOpType.TrackOpType
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableResult
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.delegation.Parser
import org.apache.flink.table.operations.{ModifyOperation, QueryOperation}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{AbstractSeq, mutable}
import scala.compat.java8.OptionConverters.RichOptionalGeneric
import scala.concurrent.ExecutionContext
import scala.util.control.Breaks.break
import scala.util.{Failure, Success, Try}

/**
 * Flink sqls serial executor actor.
 *
 * todo operation history
 *
 * @author Al-assad
 */
object SqlSerialExecutor {

  sealed trait Command
  final case class ExecuteSqls(sqlStatements: String, replyTo: ActorRef[Either[ExecReject, SerialStmtsResult]]) extends Command
  final case object CancelQueryInProcess extends Command

  sealed trait Internal extends Command
  private final case object NonImmediateOpProcessDone extends Internal

  // Internal TableResult collection process events
  sealed trait CollectResult extends Internal
  private final case class ResetRsBuffer(opType: TrackOpType, statement: String) extends CollectResult
  private final case class EmitError(error: Error) extends CollectResult
  private final case class EmitJobId(jobId: String) extends CollectResult
  private final case class EmitResultColumns(cols: Seq[Column]) extends CollectResult
  private final case class EmitResultRow(row: RowData) extends CollectResult


  def apply(sessionId: String, props: ExecConfig): Behavior[Command] = Behaviors.setup { implicit ctx =>
    ctx.log.info(s"SqlSerialExecutor[$sessionId] actor created.")
    new SqlSerialExecutor(sessionId, props).init().action()
  }
}


class SqlSerialExecutor(sessionId: String, props: ExecConfig)(implicit ctx: ActorContext[Command]) {

  import SqlSerialExecutor._

  // todo replace with a standalone scheduler
  implicit val ec: ExecutionContext = ctx.system.executionContext

  private var flinkCtx: FlinkContext = _
  private var inProcessSignal: Option[ProcessSignal] = None
  private var rsBuffer: Option[ResultBuffer] = None

  /**
   * Initialize resource for executor
   */
  def init(): SqlSerialExecutor = {
    flinkCtx = initFlinkContext(props)
    ctx.log.info(s"SqlSerialExecutor[$sessionId] initialization completed.")
    this
  }

  /**
   * Initialize flink context from ExecConfig
   */
  private def initFlinkContext(props: ExecConfig): FlinkContext = {
    val config = Configuration.fromMap(props.flinkConfig.asJava)
    val env = new StreamExecutionEnvironment(config)
    val tEnv = StreamTableEnvironment.create(env)
    val tEnvInternal = tEnv.asInstanceOf[TableEnvironmentInternal]
    val parser = tEnvInternal.getParser
    FlinkContext(tEnv, tEnvInternal, parser)
  }


  /**
   * Activate actor behavior
   */
  def action(): Behavior[Command] = Behaviors.receiveMessage {

    case ExecuteSqls(statements, replyTo) =>
      // when the previous flink modify or query operation is not done,
      // it's not allowed to execute new operation.
      inProcessSignal match {
        case Some(signal) => replyTo ! Left(
          BusyInProcess(
            "The executor is busy in process, please cancel it or wait until it is complete",
            signal.statement, signal.startTs))
          return Behaviors.same
      }
      val startTs = curTs
      // split statements by semicolon
      val stmts = FlinkSqlParser.extractSqlStatements(statements)
      if (stmts.isEmpty) {
        replyTo ! Right(SerialStmtsResult(Seq.empty, TrackOpType.NONE, startTs, curTs))
        return Behaviors.same
      }
      // parse and execute immediate statements
      val (execRs, stashOps) = parseAndExecImmediateOps(stmts)
      if (stashOps.isEmpty || execRs.exists(_.rs.isLeft)) {
        replyTo ! Right(SerialStmtsResult(execRs, TrackOpType.NONE, startTs, curTs))
        return Behaviors.same
      }
      // execute stashed operations
      stashOps.toEither match {
        case Left((stmt, queryOp)) =>
          execRs += SingleStmtResult.success(stmt, SubmitQueryOpDone)
          replyTo ! Right(SerialStmtsResult(execRs, TrackOpType.QUERY, startTs, curTs))

          // clear result buffer and submit query operation in future
          ctx.self ! ResetRsBuffer(TrackOpType.QUERY, stmt)
          val process = submitNonImmediateOpAndCollectRs(Left(queryOp), stmt)
          inProcessSignal = Some(ProcessSignal(TrackOpType.QUERY, stmt, process, curTs))
          ctx.pipeToSelf(process)(_ => NonImmediateOpProcessDone)

        case Right(stashModifyOps) =>
          val (stmts, modifyOps) = stashModifyOps.map(_._1).mkString(";") -> stashModifyOps.map(_._2)
          execRs += SingleStmtResult.success(stmts, SubmitModifyOpDone)
          replyTo ! Right(SerialStmtsResult(execRs, TrackOpType.MODIFY, startTs, curTs))

          // clear result buffer and submit modify operation in future
          ctx.self ! ResetRsBuffer(TrackOpType.MODIFY, stmts)
          val process = submitNonImmediateOpAndCollectRs(Right(modifyOps), stmts)
          inProcessSignal = Some(ProcessSignal(TrackOpType.MODIFY, stmts, process, curTs))
          ctx.pipeToSelf(process)(_ => NonImmediateOpProcessDone)
      }
      Behaviors.same

    case NonImmediateOpProcessDone =>
      rsBuffer.foreach { rs =>
        rs.isFinished = true
        rs.ts = curTs
      }
      inProcessSignal = None
      Behaviors.same

    case CancelQueryInProcess =>
      inProcessSignal.foreach(_.process.cancel(true))
      inProcessSignal = None
      rsBuffer.foreach { rs =>
        rs.isFinished = true
        rs.ts = curTs
      }
      Behaviors.same

    case _: CollectResult => collectResultBehavior()

  }.receiveSignal {
    case (context, PostStop) =>
      context.log.info(s"SqlSerialExecutor[$sessionId] stopped.")
      inProcessSignal.foreach { signal =>
        signal.process.cancel(true)
        context.log.info(s"SqlSerialExecutor[$sessionId] interrupt in-process statement: ${signal.statement}")
      }
      Behaviors.same
  }

  /**
   * TableResult collecting behavior
   */
  private def collectResultBehavior(): Behavior[Command] = Behaviors.receiveMessage {

    case ResetRsBuffer(opType, stmt) =>
      val rowsBuffer = props.resultCollectStrategy match {
        case RsCollectStrategy(EvictStrategy.DROP_HEAD, limit) => FiniteQueue[RowData](limit)
        case RsCollectStrategy(EvictStrategy.DROP_TAIL, limit) => new ArrayBuffer[RowData](limit + 10)
      }
      rsBuffer = Some(ResultBuffer(opType, stmt, rows = rowsBuffer, startTs = inProcessSignal.get.startTs))
      Behaviors.same

    case EmitError(error) =>
      rsBuffer.foreach { rs =>
        rs.error = Some(error)
        rs.isFinished = true
        rs.ts = curTs
      }
      Behaviors.same

    case EmitJobId(jobId) =>
      rsBuffer.foreach { rs =>
        rs.jobId = jobId
        rs.ts = curTs
      }
      Behaviors.same

    case EmitResultColumns(cols) =>
      rsBuffer.foreach { rs =>
        rs.cols = cols
        rs.ts = curTs
      }
      Behaviors.same

    case EmitResultRow(row) =>
      rsBuffer.foreach { rs =>
        rs.rows += row
        rs.ts = curTs
      }
      Behaviors.same
  }


  /**
   * Parse sql statements to Flink Operation, then execute all of them except for the
   * [[QueryOperation]] and [[ModifyOperation]] which will be stashed in [[StashOpToken]].
   */
  private def parseAndExecImmediateOps(stmts: Seq[String]): (mutable.Buffer[SingleStmtResult], StashOpToken) = {
    val execRs = mutable.Buffer.empty[SingleStmtResult]
    val stashToken = StashOpToken()
    for (stmt <- stmts) {
      // when a QueryOperation has been staged, the remaining statement would be skipped.
      if (stashToken.queryOp.isDefined) break
      // parse statement
      val op = Try(flinkCtx.parser.parse(stmt).get(0)).foldIdentity { err =>
        execRs += SingleStmtResult.fail(stmt, Error(s"Fail to parse statement: $stmt", err))
        break
      }
      // execute statement
      op match {
        case op: QueryOperation => stashToken.queryOp = Some(stmt -> op)
        case op: ModifyOperation => stashToken.modifyOps += stmt -> op
        case op =>
          // when a ModifyOperation has been staged, the remaining normal statement would be skipped.
          if (stashToken.modifyOps.nonEmpty) break
          val tableResult: TableResult = Try(flinkCtx.tEnvInternal.executeInternal(op)).foldIdentity { err =>
            execRs += SingleStmtResult.fail(stmt, Error(s"Fail to execute statement: $stmt", err))
            break
          }
          // collect result from flink TableResult immediately
          val cols = FlinkApiCovertTool.extractSchema(tableResult)
          val rows = Using(tableResult.collect)(iter => iter.asScala.map(row => FlinkApiCovertTool.covertRow(row)).toSeq)
            .foldIdentity { err =>
              execRs += SingleStmtResult.fail(stmt, Error(s"Fail to collect result from statement: $stmt", err))
              break
            }
          execRs += SingleStmtResult.success(stmt, ImmediateOpDone(TableResultData(cols, rows)))
      }
    }
    (execRs, stashToken)
  }


  /**
   * Submit and execute flink query or modify operation in the cancelled future.
   */
  private def submitNonImmediateOpAndCollectRs(op: Either[QueryOperation, Seq[ModifyOperation]],
                                               stmt: String): CancellableFuture[Done] = CancellableFuture {
    val tryGetTableResult = Try {
      op match {
        case Left(queryOp) => flinkCtx.tEnvInternal.executeInternal(queryOp)
        case Right(modifyOps) => flinkCtx.tEnvInternal.executeInternal(modifyOps.asJava)
      }
    }
    tryGetTableResult match {
      case Failure(err) =>
        ctx.self ! EmitError(Error(s"Fail to execute the query statement: $stmt", err))
        Done.done

      case Success(tableResult) =>
        val jobId: String = tableResult.getJobClient.asScala.map(_.getJobID.toString).getOrElse("")
        ctx.self ! EmitJobId(jobId)
        val cols = FlinkApiCovertTool.extractSchema(tableResult)
        ctx.self ! EmitResultColumns(cols)

        Using(tableResult.collect) { iter =>
          props.resultCollectStrategy match {
            case RsCollectStrategy(EvictStrategy.DROP_TAIL, limit) =>
              iter.asScala.take(limit).foreach(row => ctx.self ! EmitResultRow(FlinkApiCovertTool.covertRow(row)))
            case _ =>
              iter.asScala.foreach(row => ctx.self ! EmitResultRow(FlinkApiCovertTool.covertRow(row)))
          }
        } match {
          case Failure(err) =>
            ctx.self ! EmitError(Error(s"Fail to collect the result from statement: $stmt", err))
            Done
          case Success(_) => Done
        }
    }
  }


  /**
   * Flink environment context.
   */
  private case class FlinkContext(tEnv: StreamTableEnvironment, tEnvInternal: TableEnvironmentInternal, parser: Parser)

  /**
   * Temporary storage for the Flink Operation that requires for remote submission.
   */
  private case class StashOpToken(var queryOp: Option[(String, QueryOperation)] = None,
                                  modifyOps: mutable.Buffer[(String, ModifyOperation)] = mutable.Buffer.empty) {

    def isEmpty: Boolean = queryOp.isEmpty && modifyOps.isEmpty
    def toEither: Either[(String, QueryOperation), Seq[(String, ModifyOperation)]] =
      if (queryOp.isDefined) Left(queryOp.get) else Right(modifyOps)
  }

  /**
   * Non-immediate operation tracking signal for ModifyOperation and QueryOperation.
   */
  private case class ProcessSignal(opType: TrackOpType, statement: String, process: CancellableFuture[Done], startTs: Long)


  type DataRowBuffer = AbstractSeq[RowData] with mutable.Builder[RowData, AbstractSeq[RowData]]

  /**
   * Flink TableResult buffer for non-immediate operation.
   */
  private case class ResultBuffer(opType: TrackOpType, statement: String,
                                  var jobId: String = "",
                                  var cols: Seq[Column] = Seq.empty,
                                  rows: DataRowBuffer,
                                  var error: Option[Error] = None,
                                  var isFinished: Boolean = false,
                                  startTs: Long,
                                  var ts: Long = curTs)
}

