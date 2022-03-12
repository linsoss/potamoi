package com.github.potamois.potamoi.gateway.flink

import akka.Done
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.potamois.potamoi.commons.TryImplicits.Wrapper
import com.github.potamois.potamoi.commons.{CancellableFuture, Using, curTs}
import com.github.potamois.potamoi.gateway.flink.SqlSerialExecutor._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableResult
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.delegation.Parser
import org.apache.flink.table.operations.{ModifyOperation, QueryOperation}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.compat.java8.OptionConverters.RichOptionalGeneric
import scala.concurrent.ExecutionContext
import scala.util.control.Breaks.break
import scala.util.{Failure, Success, Try}

/**
 * Flink sqls serial executor actor.
 *
 * @author Al-assad
 */
object SqlSerialExecutor {

  sealed trait Command
  final case class ExecuteSqls(sqlStatements: String, replyTo: ActorRef[SerialStmtsResult]) extends Command

  sealed trait Internal extends Command
  final case object RemoteOperationProcessDone extends Internal

  def apply(sessionId: String, props: ExecConfig): Behavior[Command] = Behaviors.setup { ctx =>
    new SqlSerialExecutor(sessionId, props, ctx)
      .init()
      .action()
  }
}


class SqlSerialExecutor(sessionId: String,
                        props: ExecConfig,
                        ctx: ActorContext[Command]) {
  // todo replace with a standalone scheduler
  implicit val ec: ExecutionContext = ctx.system.executionContext

  private var flinkCtx: FlinkContext = _
  private var inProcessSignal: Option[ProcessSignal] = None

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

    case RemoteOperationProcessDone =>
      // release the ProcessSignal
      // todo release the remote actor resource
      inProcessSignal = None
      ctx.log.info(s"SqlSerialExecutor's remote process done, it is ready to receive new non-immediate operation. " +
                   s"[sessionId]=$sessionId " +
                   s"[stmt]=${inProcessSignal.map(_.stmt).getOrElse("")}")
      Behaviors.same

    case ExecuteSqls(statements, replyTo) =>
      // todo check inProcessSignal

      val startTs = curTs
      // split statements by semicolon
      val stmts = FlinkSqlParser.extractSqlStatements(statements)
      if (stmts.isEmpty) {
        replyTo ! SerialStmtsResult(Seq.empty, TrackOpType.NONE, startTs, curTs)
        return Behaviors.same
      }
      // parse and execute immediate statements
      val (execRs, stashOps) = parseAndExecImmediateOps(stmts)
      if (stashOps.isEmpty || execRs.exists(_.rs.isLeft)) {
        replyTo ! SerialStmtsResult(execRs, TrackOpType.NONE, startTs, curTs)
        return Behaviors.same
      }
      // execute stashed operations
      stashOps.toEither match {

        // query operations
        case Left((stmt, stashQueryOp)) =>
          // spawn TableResult collector actor
          val collector = ctx.spawn(QueryOpRsCollector(sessionId, props.resultCollectStrategy),
            s"flinkSqlSerialExecutor-collector-queryOps-$sessionId")
          ctx.watch(collector)
          execRs += SingleStmtResult.success(stmt, SubmitQueryOpDone(collector))

          replyTo ! SerialStmtsResult(execRs, TrackOpType.QUERY, startTs, curTs)

        // modify operations
        case Right(stashModifyOps) =>
          lazy val stmts = stashModifyOps.map(_._1).mkString(";")
          val modifyOps = stashModifyOps.map(_._2)
          // spawn TableResult collector actor
          val collector = ctx.spawn(ModifyOpRsCollector(sessionId), s"flinkSqlSerialExecutor-collector-modifyOps-$sessionId")
          ctx.watch(collector)
          execRs += SingleStmtResult.success(stmts, SubmitModifyOpDone(collector))

          // exec modify operations in future
          val process = submitModifyOpsAndCollectRs(modifyOps, stmts, collector)
          inProcessSignal = Some(ProcessSignal.modifyOp(collector, process, stmts))
          ctx.pipeToSelf(process)(_ => RemoteOperationProcessDone)
          replyTo ! SerialStmtsResult(execRs, TrackOpType.MODIFY, startTs, curTs)
      }
      Behaviors.same
  }


  /**
   * Parse sql statements to Flink Operation, then execute all of them except for the [[QueryOperation]] and [[ModifyOperation]],
   * which will be stashed in [[StashOpToken]].
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
   * todo
   */
  //noinspection DuplicatedCode
  private def submitModifyOpsAndCollectRs(modifyOps: Seq[ModifyOperation],
                                          stmts: String,
                                          collector: ActorRef[ModifyOpRsCollector.Command]): CancellableFuture[Done] = CancellableFuture {
    import ModifyOpRsCollector._

    Try(flinkCtx.tEnvInternal.executeInternal(modifyOps.asJava)) match {
      case Failure(err) =>
        collector ! EmitError(Error(s"Fail to execute the modify statements: $stmts", err))
        Done.done

      case Success(tableResult) =>
        val jobId: String = tableResult.getJobClient.asScala.map(_.getJobID.toString).getOrElse("")
        collector ! EmitJobId(jobId)
        val cols = FlinkApiCovertTool.extractSchema(tableResult)

        Using(tableResult.collect)(iter => iter.asScala.map(row => FlinkApiCovertTool.covertRow(row)).toSeq) match {
          case Failure(err) =>
            collector ! EmitError(Error(s"Fail to collect the result from statements: $stmts", err))
            Done.done
          case Success(rows) =>
            collector ! EmitResultData(TableResultData(cols, rows))
            Done.done
        }
    }
  }

  //noinspection DuplicatedCode
  private def submitQueryOpAndCollectRs(queryOp: ModifyOperation, stmt: String,
                                        collector: ActorRef[QueryOpRsCollector.Command]): CancellableFuture[Done] = CancellableFuture {
    import QueryOpRsCollector._

    Try(flinkCtx.tEnvInternal.executeInternal(queryOp)) match {
      case Failure(err) =>
        collector ! EmitError(Error(s"Fail to execute the query statement: $stmt", err))
        Done.done

      case Success(tableResult) =>
        val jobId: String = tableResult.getJobClient.asScala.map(_.getJobID.toString).getOrElse("")
        collector ! EmitJobId(jobId)
        val cols = FlinkApiCovertTool.extractSchema(tableResult)
        collector ! EmitResultColumns(cols)

        Using(tableResult.collect) { iter =>
          props.resultCollectStrategy match {
            case RsCollectStrategy(EvictStrategy.DROP_TAIL, limit) =>
              iter.asScala.take(limit).foreach(row => collector ! EmitResultRow(FlinkApiCovertTool.covertRow(row)))
            case _ =>
              iter.asScala.foreach(row => collector ! EmitResultRow(FlinkApiCovertTool.covertRow(row)))
          }
          collector ! EmitCollectRowsDone
        } match {
          case Failure(err) =>
            collector ! EmitError(Error(s"Fail to collect the result from statement: $stmt", err))
            Done
          case Success(_) => Done
        }
    }
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
   * Flink environment context.
   */
  private case class FlinkContext(tEnv: StreamTableEnvironment, tEnvInternal: TableEnvironmentInternal, parser: Parser)

  /**
   * Signal of Non-immediate operation execution process.
   */
  private case class ProcessSignal(collector: Either[ActorRef[ModifyOpRsCollector.Command], ActorRef[QueryOpRsCollector.Command]],
                                   process: CancellableFuture[Done],
                                   stmt: String,
                                   launchTs: Long)
  private object ProcessSignal {

    def modifyOp(collector: ActorRef[ModifyOpRsCollector.Command], process: CancellableFuture[Done], stmt: String): ProcessSignal =
      ProcessSignal(Left(collector), process, stmt, curTs)

    def queryOp(collector: ActorRef[QueryOpRsCollector.Command], process: CancellableFuture[Done], stmt: String): ProcessSignal =
      ProcessSignal(Right(collector), process, stmt, curTs)
  }

}
