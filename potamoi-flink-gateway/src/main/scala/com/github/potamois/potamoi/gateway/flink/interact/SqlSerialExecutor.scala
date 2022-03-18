package com.github.potamois.potamoi.gateway.flink.interact

import akka.Done
import akka.actor.typed.pubsub.Topic
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior, PostStop}
import com.github.potamois.potamoi.commons.{CancellableFuture, FiniteQueue, RichMutableMap, RichString, RichTry, Using, curTs}
import com.github.potamois.potamoi.gateway.flink.interact.OpType.OpType
import com.github.potamois.potamoi.gateway.flink.interact.SqlSerialExecutor.Command
import com.github.potamois.potamoi.gateway.flink.parser.FlinkSqlParser
import com.github.potamois.potamoi.gateway.flink.{Error, FlinkApiCovertTool, PageReq, PageRsp, interact}
import com.typesafe.scalalogging.Logger
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.execution.JobClient
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableResult
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.delegation.Parser
import org.apache.flink.table.operations.{ModifyOperation, QueryOperation}
import org.apache.flink.table.planner.operations.PlannerQueryOperation

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{AbstractSeq, TraversableLike, mutable}
import scala.compat.java8.OptionConverters.RichOptionalGeneric
import scala.concurrent.ExecutionContextExecutor
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success, Try}

/**
 * Flink sqls serial executor actor.
 *
 * The format of the submitted flink job name is "potamoi-fsi-{sessionId}"
 * such as "potamoi-fsi-1234567890"
 *
 * @author Al-assad
 */
object SqlSerialExecutor {

  type RejectableDone = Either[ExecReqReject, Done]

  sealed trait Command
  final case class ExecuteSqls(sqlStatements: String, props: ExecConfig, replyTo: ActorRef[RejectableDone]) extends Command
  final case class IsInProcess(replyTo: ActorRef[Boolean]) extends Command
  final case object CancelCurProcess extends Command

  /**
   * Subscribe the result change events from this executor, see [[ResultChange]].
   */
  final case class SubscribeState(listener: ActorRef[ResultChange]) extends Command
  /**
   * Unsubscribe the result change events from this executor.
   */
  final case class UnsubscribeState(listener: ActorRef[ResultChange]) extends Command

  // Query result command
  sealed trait QueryResult extends Command
  // Get the snapshot result of current sqls plan that has been executed.
  final case class GetExecPlanRsSnapshot(replyTo: ActorRef[Option[SerialStmtsResult]]) extends QueryResult
  // Get the snapshot TableResult that has been collected.
  final case class GetQueryRsSnapshot(limit: Int = -1, replyTo: ActorRef[Option[TableResultSnapshot]]) extends QueryResult
  // Get the snapshot collected TableResult by pagination.
  final case class GetQueryRsSnapshotByPage(page: PageReq, replyTo: ActorRef[Option[PageableTableResultSnapshot]]) extends QueryResult


  sealed trait Internal extends Command
  private final case class ProcessFinished(replyTo: ActorRef[Either[ExecReqReject, Done]]) extends Internal
  private final case class SingleStmtFinished(result: SingleStmtResult) extends Internal
  private final case class InitQueryRsBuffer(collStrategy: RsCollectStrategy) extends Internal
  private final case class CollectQueryOpColsRs(cols: Seq[Column]) extends Internal
  private final case class CollectQueryOpRow(row: Row) extends Internal
  private final case class ErrorWhenCollectQueryOpRow(error: Error) extends Internal
  private final case class HookFlinkJobClient(jobClient: JobClient) extends Internal


  def apply(sessionId: String): Behavior[Command] = Behaviors.setup { implicit ctx =>
    ctx.log.info(s"SqlSerialExecutor[$sessionId] actor created.")
    new SqlSerialExecutor(sessionId).action()
  }
}


class SqlSerialExecutor(sessionId: String)(implicit ctx: ActorContext[Command]) {

  import ResultChangeEvent._
  import SqlSerialExecutor._

  // Execution context for CancelableFuture
  // todo replace with standalone dispatcher
  implicit val ec: ExecutionContextExecutor = ctx.system.executionContext
  // Cancelable process log, Plz use this log when it need to output logs inside CancelableFuture
  private val pcLog: Logger = Logger(getClass)

  // result change topic
  protected val rsChangeTopic: ActorRef[Topic.Command[ResultChange]] = ctx.spawn(Topic[ResultChange](
    topicName = s"potamoi-fsi-exec-$sessionId"),
    name = s"potamoi-fsi-exec-topic-$sessionId"
  )

  // running process
  private var process: Option[CancellableFuture[Done]] = None
  // executed statements result buffer
  private var rsBuffer: Option[StmtsRsBuffer] = None
  // collected table result buffer
  private var queryRsBuffer: Option[QueryRsBuffer] = None
  // hook flink job client for force cancel job if necessary
  private var jobClientHook: Option[JobClient] = None

  // default flink job name
  protected val defaultJobName = s"potamoi-fsi-$sessionId"

  def action(): Behavior[Command] = Behaviors.receiveMessage[Command] {
    case IsInProcess(replyTo) =>
      replyTo ! process.isDefined
      Behaviors.same

    case CancelCurProcess =>
      if (process.isDefined) {
        process.get.cancel(interrupt = true)
        process = None
        ctx.log.info(s"session[$sessionId] current process cancelled.")
        ctx.self ! ProcessFinished(ctx.system.ignoreRef)
      }
      Behaviors.same

    case ExecuteSqls(statements, props, replyTo) => process match {
      // when the previous statements execution process has not been done,
      // it's not allowed to execute new operation.
      case Some(_) =>
        val rejectReason = BusyInProcess(
          "The executor is busy in process, please cancel it first or wait until it is complete",
          rsBuffer.map(_.startTs).get
        )
        rsChangeTopic ! Topic.Publish(RejectStmtsExecPlan(statements, rejectReason))
        replyTo ! Left(rejectReason)
        Behaviors.same

      case None =>
        // extract effective execution config
        val effectProps = props.toEffectiveExecConfig.updateFlinkConfig { conf =>
          // set flink job name
          conf ?+= "pipeline.name" -> defaultJobName
        }
        // split sql statements and execute each one
        val stmtsPlan = FlinkSqlParser.extractSqlStatements(statements)
        rsChangeTopic ! Topic.Publish(AcceptStmtsExecPlan(stmtsPlan, effectProps.flinkConfig))

        stmtsPlan match {
          case stmts if stmts.isEmpty =>
            replyTo ! Right(Done)
          case stmts =>
            // reset result buffer
            rsBuffer = Some(StmtsRsBuffer(mutable.Buffer.empty, curTs))
            queryRsBuffer = None
            // parse and execute statements in cancelable future
            process = Some(CancellableFuture(execStatementsPlan(stmts, effectProps)))
            ctx.pipeToSelf(process.get)(_ => ProcessFinished(replyTo))
        }
        Behaviors.same
    }

    case SubscribeState(listener) =>
      rsChangeTopic ! Topic.Subscribe(listener)
      Behaviors.same

    case UnsubscribeState(listener) =>
      rsChangeTopic ! Topic.Unsubscribe(listener)
      Behaviors.same

    case cmd: Internal => internalBehavior(cmd)
    case cmd: QueryResult => queryResultBehavior(cmd)

  }.receiveSignal {
    case (context, PostStop) =>
      // release resources before stop
      process.foreach { ps =>
        ps.cancel(true)
        context.log.info(s"SqlSerialExecutor[$sessionId] interrupt the running statements execution process.")
      }
      Try(jobClientHook.map(_.cancel))
        .failed.foreach(ctx.log.error(s"session[$sessionId] Fail to cancel from Flink JobClient.", _))
      context.log.info(s"SqlSerialExecutor[$sessionId] stopped.")
      Behaviors.same
  }


  /**
   * [[Internal]] command received behavior
   */
  private def internalBehavior(command: Internal): Behavior[Command] = command match {

    case HookFlinkJobClient(jobClient) =>
      jobClientHook = Some(jobClient)
      ctx.log.info(s"SqlSerialExecutor[$sessionId] Hooked Flink JobClient, jobId=${jobClient.getJobID.toString}.")
      Behaviors.same

    case ProcessFinished(replyTo) =>
      replyTo ! Right(Done)
      process = None
      queryRsBuffer.foreach { buf =>
        buf.isFinished = true
        buf.ts = curTs
      }
      rsChangeTopic ! Topic.Publish(AllStmtsDone(rsBuffer.map(_.toSerialStmtsResult(true)).orNull))
      // cancel flink job if necessary
      Try(jobClientHook.map(_.cancel))
        .failed.foreach(ctx.log.error(s"session[$sessionId] Fail to cancel from Flink JobClient.", _))
      Behaviors.same

    case SingleStmtFinished(stmtRs) =>
      rsBuffer.foreach { buffer =>
        buffer.result += stmtRs
      }
      rsChangeTopic ! Topic.Publish(SingleStmtDone(stmtRs))
      Behaviors.same

    case InitQueryRsBuffer(strategy) =>
      val rowsBuffer: DataRowBuffer = strategy match {
        case RsCollectStrategy(EvictStrategy.DROP_HEAD, limit) => FiniteQueue[Row](limit)
        case RsCollectStrategy(EvictStrategy.DROP_TAIL, limit) => new ArrayBuffer[Row](limit + 10)
        case _ => new ArrayBuffer[Row]()
      }
      queryRsBuffer = Some(QueryRsBuffer(rows = rowsBuffer, startTs = curTs))
      Behaviors.same

    case CollectQueryOpColsRs(cols) =>
      queryRsBuffer.foreach { buf =>
        buf.cols = cols
        buf.ts = curTs
      }
      rsChangeTopic ! Topic.Publish(ReceiveQueryOpColumns(cols))
      Behaviors.same

    case CollectQueryOpRow(row) =>
      queryRsBuffer.foreach { buf =>
        buf.rows += row
        buf.ts = curTs
      }
      rsChangeTopic ! Topic.Publish(ReceiveQueryOpRow(row))
      Behaviors.same

    case ErrorWhenCollectQueryOpRow(err) =>
      queryRsBuffer.foreach { buf =>
        buf.error = Some(err)
        buf.ts = curTs
      }
      rsChangeTopic ! Topic.Publish(ErrorDuringQueryOp(err))
      Behaviors.same
  }


  /**
   * [[QueryResult]] command received behavior
   */
  private def queryResultBehavior(command: QueryResult): Behavior[Command] = command match {

    case GetExecPlanRsSnapshot(replyTo) =>
      val snapshot = rsBuffer.map(_.toSerialStmtsResult(process.isEmpty))
      replyTo ! snapshot
      Behaviors.same

    case GetQueryRsSnapshot(limit, replyTo) =>
      val snapshot = queryRsBuffer match {
        case None => None
        case Some(buf) =>
          val rows = limit match {
            case Int.MaxValue => buf.rows
            case size if size < 0 => buf.rows
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
              totalRows = rowsCount,
              hasNext = pageIndex < pages - 1,
              data = TableResultData(buf.cols, Seq(rowsSlice: _*))
            )
          }
          Some(interact.PageableTableResultSnapshot(
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
      case Left(_) => Done
      case Right(stashOp) => if (stashOp.isEmpty) Done else execStashedOps(stashOp, effectProps.rsCollectSt)
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
            rsChangeTopic ! Topic.Publish(SingleStmtStart(stmt))
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
            ctx.self ! SingleStmtFinished(SingleStmtResult.success(stmt, ImmediateOpDone(interact.TableResultData(cols, rows))))
        }
      }
    }
    Right(stashToken)
  }

  /**
   * Execute the stashed non-immediate operations such as  [[QueryOperation]] and [[ModifyOperation]],
   * and collect the result from TableResult.
   *
   * This process can lead to long thread blocking.
   */
  //noinspection DuplicatedCode
  private def execStashedOps(stashOp: StashOpToken, rsCollStrategy: RsCollectStrategy)
                            (implicit flinkCtx: FlinkContext): Done = stashOp.toEither match {
    case Right(stashModifyOps) =>
      val (stmts, modifyOps) = stashModifyOps.map(_._1).mkString(";") -> stashModifyOps.map(_._2)
      rsChangeTopic ! Topic.Publish(SingleStmtStart(stmts))
      Try(flinkCtx.tEnvInternal.executeInternal(modifyOps.asJava)) match {
        case Failure(err) =>
          ctx.self ! SingleStmtFinished(SingleStmtResult.fail(stmts, Error(s"Fail to execute modify statements: ${stmts.compact}", err)))
          Done
        case Success(tableResult) =>
          val jobClient: Option[JobClient] = tableResult.getJobClient.asScala
          ctx.self ! HookFlinkJobClient(jobClient.get)

          val jobId: Option[String] = jobClient.map(_.getJobID.toString)
          ctx.self ! SingleStmtFinished(SingleStmtResult.success(stmts, SubmitModifyOpDone(jobId.get)))
          rsChangeTopic ! Topic.Publish(SubmitJobToFlinkCluster(OpType.MODIFY, jobId.get, defaultJobName))
          // blocking until the insert operation job is finished
          jobClient.get.getJobExecutionResult.get()
          Done
      }

    case Left((stmt, queryOp)) =>
      rsChangeTopic ! Topic.Publish(SingleStmtStart(stmt))
      Try(flinkCtx.tEnvInternal.executeInternal(queryOp)) match {
        case Failure(err) =>
          ctx.self ! SingleStmtFinished(SingleStmtResult.fail(stmt, Error(s"Fail to execute query statement: ${stmt.compact}", err)))
          Done

        case Success(tableResult) =>
          val jobClient: Option[JobClient] = tableResult.getJobClient.asScala
          ctx.self ! HookFlinkJobClient(jobClient.get)

          val jobId: Option[String] = jobClient.map(_.getJobID.toString)
          ctx.self ! SingleStmtFinished(SingleStmtResult.success(stmt, SubmitQueryOpDone(jobId.get)))
          ctx.self ! InitQueryRsBuffer(rsCollStrategy)
          rsChangeTopic ! Topic.Publish(SubmitJobToFlinkCluster(OpType.QUERY, jobId.get, defaultJobName))

          val cols = FlinkApiCovertTool.extractSchema(tableResult)
          ctx.self ! CollectQueryOpColsRs(cols)

          // get the topmost fetch rex from query operation
          val limitRex = queryOp match {
            case op: PlannerQueryOperation => FlinkSqlParser.getTopmostLimitRexFromOp(op)
            case _ => None
          }
          // blocking until the select operation job is finished
          Using(tableResult.collect) { iter =>
            val stream = rsCollStrategy match {
              case RsCollectStrategy(EvictStrategy.DROP_TAIL, limit) => iter.asScala.take(limit.min(limitRex.getOrElse(Int.MaxValue)))
              case RsCollectStrategy(EvictStrategy.DROP_HEAD, _) =>
                if (limitRex.isDefined) iter.asScala.take(limitRex.get) else iter.asScala
              case _ => iter.asScala
            }
            stream.foreach(row => ctx.self ! CollectQueryOpRow(FlinkApiCovertTool.covertRow(row)))
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

    // convert to SerialStmtsResult
    def toSerialStmtsResult(finished: Boolean): SerialStmtsResult = SerialStmtsResult(
      result = Seq(result: _*),
      isFinished = finished,
      lastOpType = lastOpType,
      startTs = startTs,
      lastTs = lastTs
    )
  }

  private type DataRowBuffer = AbstractSeq[Row] with mutable.Builder[Row, AbstractSeq[Row]] with TraversableLike[Row, AbstractSeq[Row]]

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
