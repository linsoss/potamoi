package com.github.potamois.potamoi.flinkgateway

import akka.Done
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.potamois.potamoi.commons.StringImplicits.StringWrapper
import com.github.potamois.potamoi.commons.TryImplicits.Wrapper
import com.github.potamois.potamoi.commons.{CancellableFuture, FiniteQueue, Using, curTs}
import com.github.potamois.potamoi.flinkgateway.ExptExecutor.Command
import com.github.potamois.potamoi.gateway.flink.OpType.OpType
import com.github.potamois.potamoi.gateway.flink._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableResult
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.delegation.Parser
import org.apache.flink.table.operations.{ModifyOperation, QueryOperation}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{AbstractSeq, TraversableLike, mutable}
import scala.compat.java8.OptionConverters.RichOptionalGeneric
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success, Try}

/**
 * Flink sqls serial executor actor.
 *
 * @author Al-assad
 */
object ExptExecutor {

  sealed trait Command
  // todo
  //  final case class Subscribe(actorRef: ActorRef[_]) extends Command
  final case class ExecuteSqls(sqlStatements: String, props: ExecConfig, replyTo: ActorRef[Done]) extends Command
  final case class IsInProcess(replyTo: ActorRef[Boolean]) extends Command
  final case object CancelCurProcess extends Command

  sealed trait QueryResult extends Command
  // Get the snapshot result of current sqls plan that has been executed.
  final case class GetExecPlanRsSnapshot(replyTo: ActorRef[Option[SerialStmtsResult]]) extends QueryResult
  // Get the snapshot TableResult that has been collected.
  final case class GetQueryRsSnapshot(limit: Int = -1, replyTo: ActorRef[Option[TableResultSnapshot]]) extends QueryResult
  // Get the snapshot collected TableResult by pagination.
  final case class GetQueryRsSnapshotByPage(page: PageReq, replyTo: ActorRef[Option[PageableTableResultSnapshot]]) extends QueryResult

  sealed trait Internal extends Command
  private final case class ProcessFinished(replyTo: ActorRef[Done]) extends Internal
  private final case class SingleStmtFinished(result: SingleStmtResult) extends Internal
  private final case class InitQueryRsBuffer(collStrategy: RsCollectStrategy) extends Internal
  private final case class CollectQueryOpColsRs(cols: Seq[Column]) extends Internal
  private final case class CollectQueryOpRow(row: Row) extends Internal
  private final case class ErrorWhenCollectQueryOpRow(error: Error) extends Internal


  def apply(sessionId: String): Behavior[Command] = Behaviors.setup { implicit ctx =>
    new ExptExecutor(sessionId).action()
  }
}


import com.github.potamois.potamoi.flinkgateway.ExptExecutor._

class ExptExecutor(sessionId: String)(implicit ctx: ActorContext[Command]) {

  // todo replace with standalone dispatcher
  implicit val ec = ctx.system.executionContext

  private var process: Option[CancellableFuture[Done]] = None
  private var rsBuffer: Option[StmtsRsBuffer] = None
  private var queryRsBuffer: Option[QueryRsBuffer] = None

  def action(): Behavior[Command] = Behaviors.receiveMessage {

    case IsInProcess(replyTo) =>
      replyTo ! process.isDefined
      Behaviors.same

    case CancelCurProcess =>
      process.foreach(_.cancel(interrupt = true))
      ctx.log.info(s"session[$sessionId] current process cancelled.")
      ctx.self ! ProcessFinished(ctx.system.ignoreRef)
      Behaviors.same

    case ExecuteSqls(statements, props, replyTo) =>
      // extract effective execution config
      val effectProps = props.toEffectiveExecConfig
      //  split sql statements and execute each one
      FlinkSqlParser.extractSqlStatements(statements) match {
        case stmts if stmts.isEmpty =>
          replyTo ! Done
        case stmts =>
          // reset result buffer
          rsBuffer = Some(StmtsRsBuffer(mutable.Buffer.empty, curTs))
          queryRsBuffer = None
          // parse and execute statements in cancelable future
          process = Some(CancellableFuture(execStatementsPlan(stmts, effectProps)))
          ctx.pipeToSelf(process.get)(_ => ProcessFinished(replyTo))
      }
      Behaviors.same

    case cmd: Internal => internalBehavior(cmd)
    case cmd: QueryResult => queryResultBehavior(cmd)
  }

  /**
   * [[Internal]] command received behavior
   */
  private def internalBehavior(command: Internal): Behavior[Command] = command match {
    case ProcessFinished(replyTo) =>
      replyTo ! Done
      process = None
      queryRsBuffer.foreach { buf =>
        buf.isFinished = true
        buf.ts = curTs
      }
      Behaviors.same

    case SingleStmtFinished(stmtRs) =>
      rsBuffer.foreach { buffer =>
        buffer.result += stmtRs
        // log result
        stmtRs.rs match {
          case Left(err) =>
            ctx.log.error(s"session[$sessionId] ${err.summary}", err.stack)
          case Right(result) =>
            ctx.log.debug(s"session[$sessionId] Execute: ${stmtRs.stmt} => \n${
              result match {
                case ImmediateOpDone(data) => data.tabulateContent
                case r: SubmitModifyOpDone => r.toLog
                case r: SubmitQueryOpDone => r.toLog
              }
            }")
        }
      }
      Behaviors.same

    case InitQueryRsBuffer(strategy) =>
      val rowsBuffer: DataRowBuffer = strategy match {
        case RsCollectStrategy(EvictStrategy.DROP_HEAD, limit) => FiniteQueue[Row](limit)
        case RsCollectStrategy(EvictStrategy.DROP_TAIL, limit) => new ArrayBuffer[Row](limit + 10)
      }
      queryRsBuffer = Some(QueryRsBuffer(rows = rowsBuffer, startTs = curTs))
      Behaviors.same

    case CollectQueryOpColsRs(cols) =>
      queryRsBuffer.foreach { buf =>
        buf.cols = cols
        buf.ts = curTs
      }
      Behaviors.same

    case CollectQueryOpRow(row) =>
      queryRsBuffer.foreach { buf =>
        buf.rows += row
        buf.ts = curTs
      }
      Behaviors.same

    case ErrorWhenCollectQueryOpRow(err) =>
      queryRsBuffer.foreach { buf =>
        buf.error = Some(err)
        buf.ts = curTs
      }
      Behaviors.same
  }


  /**
   * [[QueryResult]] command received behavior
   */
  private def queryResultBehavior(command: QueryResult): Behavior[Command] = command match {
    case GetExecPlanRsSnapshot(replyTo) =>
      val snapshot = rsBuffer match {
        case None => None
        case Some(buf) => Some(SerialStmtsResult(
          result = Seq(buf.result: _*),
          isFinished = process.isEmpty,
          lastOpType = buf.lastOpType,
          startTs = buf.startTs,
          lastTs = buf.lastTs))
      }
      replyTo ! snapshot
      Behaviors.same

    case GetQueryRsSnapshot(limit, replyTo) =>
      val snapshot = queryRsBuffer match {
        case None => None
        case Some(buf) =>
          val rows = limit match {
            case Int.MaxValue | size if size < 0 => buf.rows
            case size => buf.rows.take(size)
          }
          Some(TableResultSnapshot(
            data = TableResultData(buf.cols, Seq(rows: _*)),
            error = buf.error,
            isFinished = buf.isFinished,
            lastTs = buf.ts
          ))
      }
      replyTo ! snapshot
      Behaviors.same

    case GetQueryRsSnapshotByPage(PageReq(pageIndex, pageSize), replyTo) =>
      val snapshot = queryRsBuffer match {
        case None => None
        case Some(buf) =>
          val payload = {
            val rowsCount = buf.rows.size
            val pages = (rowsCount.toDouble / pageSize).ceil.toInt
            val offset = pageIndex * pageSize
            val rowsSlice = buf.rows.slice(offset, offset + pageSize)
            PageRsp(
              index = pageIndex,
              size = rowsSlice.size,
              totalPages = pages,
              totalRow = rowsCount,
              hasNext = pageIndex < pages - 1,
              data = TableResultData(buf.cols, Seq(rowsSlice: _*))
            )
          }
          Some(PageableTableResultSnapshot(
            data = payload,
            error = buf.error,
            isFinished = buf.isFinished,
            lastTs = buf.ts))
      }
      replyTo ! snapshot
      Behaviors.same

  }


  /**
   * Execute sql statements plan.
   */
  private def execStatementsPlan(stmts: Seq[String], effectProps: EffectiveExecConfig): Done = {
    // execute stashed operations
    implicit val flinkCtx: FlinkContext = createFlinkContext(effectProps.flinkConfig)
    // parse and execute sql statements
    execImmediateOpsAndStashNonImmediateOps(stmts) match {
      case Left(done) => Done
      case Right(stashOp) => if (stashOp.isEmpty) Done else execStashedOps(stashOp, effectProps.rsCollectStrategy)
    }
  }

  /**
   * Parse all sql statements to Flink Operation, then execute all of them except for the
   * [[QueryOperation]] and [[ModifyOperation]] which will be stashed in [[StashOpToken]].
   */
  private def execImmediateOpsAndStashNonImmediateOps(stmts: Seq[String])(implicit flinkCtx: FlinkContext): Either[Done, StashOpToken] = {
    val stashToken = StashOpToken()
    breakable {
      for (stmt <- stmts) {
        if (stashToken.queryOp.isDefined) break
        // parse statement
        val op = Try(flinkCtx.parser.parse(stmt).get(0)).foldIdentity { err =>
          ctx.self ! SingleStmtFinished(SingleStmtResult.fail(stmt, Error(s"Fail to parse statement: ${stmt.compact}", err)))
          return Left(Done)
        }
        op match {
          case op: QueryOperation => stashToken.queryOp = Some(stmt -> op)
          case op: ModifyOperation => stashToken.modifyOps += stmt -> op
          case op =>
            // when a ModifyOperation has been staged, the remaining normal statement would be skipped.
            if (stashToken.modifyOps.nonEmpty) break
            val tableResult: TableResult = Try(flinkCtx.tEnvInternal.executeInternal(op)).foldIdentity { err =>
              ctx.self ! SingleStmtFinished(SingleStmtResult.fail(stmt, Error(s"Fail to execute statement: ${stmt.compact}", err)))
              return Left(Done)
            }
            // collect result from flink TableResult immediately
            val cols = FlinkApiCovertTool.extractSchema(tableResult)
            val rows = Using(tableResult.collect)(iter => iter.asScala.map(row => FlinkApiCovertTool.covertRow(row)).toSeq)
              .foldIdentity { err =>
                ctx.self ! SingleStmtFinished(SingleStmtResult.fail(stmt, Error(s"Fail to collect table result: ${stmt.compact}", err)))
                return Left(Done)
              }
            ctx.self ! SingleStmtFinished(SingleStmtResult.success(stmt, ImmediateOpDone(TableResultData(cols, rows))))
        }
      }
    }
    Right(stashToken)
  }

  /**
   * Execute the stashed non-immediate operations such as  [[QueryOperation]] and [[ModifyOperation]],
   * and collect the result from TableResult.
   */
  private def execStashedOps(stashOp: StashOpToken, rsCollStrategy: RsCollectStrategy)
                            (implicit flinkCtx: FlinkContext): Done = stashOp.toEither match {
    case Right(stashModifyOps) =>
      val (stmts, modifyOps) = stashModifyOps.map(_._1).mkString(";") -> stashModifyOps.map(_._2)
      Try(flinkCtx.tEnvInternal.executeInternal(modifyOps.asJava)) match {
        case Failure(err) =>
          ctx.self ! SingleStmtFinished(SingleStmtResult.fail(stmts, Error(s"Fail to execute modify statements: ${stmts.compact}", err)))
          Done
        case Success(tableResult) =>
          val jobId: Option[String] = tableResult.getJobClient.asScala.map(_.getJobID.toString)
          ctx.self ! SingleStmtFinished(SingleStmtResult.success(stmts, SubmitModifyOpDone(jobId.get)))
          Done
      }

    case Left((stmt, queryOp)) =>
      Try(flinkCtx.tEnvInternal.executeInternal(queryOp)) match {
        case Failure(err) =>
          ctx.self ! SingleStmtFinished(SingleStmtResult.fail(stmt, Error(s"Fail to execute query statement: ${stmt.compact}", err)))
          Done

        case Success(tableResult) =>
          val jobId: Option[String] = tableResult.getJobClient.asScala.map(_.getJobID.toString)
          ctx.self ! SingleStmtFinished(SingleStmtResult.success(stmt, SubmitModifyOpDone(jobId.get)))
          ctx.self ! InitQueryRsBuffer(rsCollStrategy)

          val cols = FlinkApiCovertTool.extractSchema(tableResult)
          ctx.self ! CollectQueryOpColsRs(cols)

          Using(tableResult.collect) { iter =>
            rsCollStrategy match {
              case RsCollectStrategy(EvictStrategy.DROP_TAIL, limit) =>
                iter.asScala.take(limit).foreach(row => ctx.self ! CollectQueryOpRow(FlinkApiCovertTool.covertRow(row)))
              case _ =>
                iter.asScala.foreach(row => ctx.self ! CollectQueryOpRow(FlinkApiCovertTool.covertRow(row)))
            }
          } match {
            case Failure(err) =>
              ctx.self ! ErrorWhenCollectQueryOpRow(Error("Fail to collect table result", err))
              Done
            case Success(_) =>
              Done
          }
      }
  }


  /**
   * Flink environment context.
   */
  private case class FlinkContext(tEnv: StreamTableEnvironment, tEnvInternal: TableEnvironmentInternal, parser: Parser)

  /**
   * Initialize flink context.
   */
  private def createFlinkContext(flinkConfig: Map[String, String]): FlinkContext = {
    val config = Configuration.fromMap(flinkConfig.asJava)
    val env = new StreamExecutionEnvironment(config)
    val tEnv = StreamTableEnvironment.create(env)
    val tEnvInternal = tEnv.asInstanceOf[TableEnvironmentInternal]
    val parser = tEnvInternal.getParser
    FlinkContext(tEnv, tEnvInternal, parser)
  }

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
   * Flink sql statements execution result buffer.
   */
  private case class StmtsRsBuffer(result: mutable.Buffer[SingleStmtResult], startTs: Long) {
    def lastTs: Long = result.lastOption.map(_.ts).getOrElse(startTs)
    def lastOpType: OpType = result.lastOption.map(_.opType).getOrElse(OpType.UNKNOWN)
  }

  type DataRowBuffer = AbstractSeq[Row] with mutable.Builder[Row, AbstractSeq[Row]] with TraversableLike[Row, AbstractSeq[Row]]

  /**
   * Flink query statements execution result buffer.
   */
  private case class QueryRsBuffer(var cols: Seq[Column] = Seq.empty,
                                   rows: DataRowBuffer,
                                   var error: Option[Error] = None,
                                   var isFinished: Boolean = false,
                                   startTs: Long,
                                   var ts: Long = curTs)

}



