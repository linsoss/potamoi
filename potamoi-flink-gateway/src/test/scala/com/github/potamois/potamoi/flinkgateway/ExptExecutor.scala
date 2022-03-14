package com.github.potamois.potamoi.flinkgateway

import akka.Done
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.github.potamois.potamoi.commons.TryImplicits.Wrapper
import com.github.potamois.potamoi.commons.{CancellableFuture, Using, curTs}
import com.github.potamois.potamoi.gateway.flink.TrackOpType.TrackOpType
import com.github.potamois.potamoi.gateway.flink._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableResult
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.delegation.Parser
import org.apache.flink.table.operations.{ModifyOperation, QueryOperation}

import scala.collection.JavaConverters._
import scala.collection.{AbstractSeq, mutable}
import scala.compat.java8.OptionConverters.RichOptionalGeneric
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success, Try}

object ExptExecutor {

  sealed trait Command
  final case class Subscribe(actorRef: ActorRef[CollectResult]) extends Command
  final case class ExecuteSqls(sqlStatements: String, props: ExecConfig, replyTo: ActorRef[Done]) extends Command

  sealed trait Internal extends Command
  final case class ProcessFinished(replyTo: ActorRef[Done]) extends Internal

  sealed trait CollectResult extends Internal
  final case class ExecError() extends CollectResult


  def apply(sessionId: String): Behavior[Command] = Behaviors.setup { implicit ctx =>

    // todo replace
    implicit val ec = ctx.system.executionContext

    var process: Option[CancellableFuture[Done]] = None
    var rsBuffer: Option[StmtsRsBuffer] = None

    Behaviors.receiveMessage {

      case ExecuteSqls(statements, props, replyTo) =>
        // covert ExecConfig and split statements
        val effectProps = props.toEffectiveExecConfig
        FlinkSqlParser.extractSqlStatements(statements) match {
          case stmts if stmts.isEmpty =>
            replyTo ! Done
          case stmts =>
            // reset result buffer
            rsBuffer = Some(StmtsRsBuffer(mutable.Buffer.empty, curTs, curTs))
            // execute statements in Future
            process = Some(CancellableFuture(execStatementsPlan(stmts, effectProps)))
            ctx.pipeToSelf(process.get)(_ => ProcessFinished(replyTo))
        }
        Behaviors.same

      case ProcessFinished(replyTo) =>
        replyTo ! Done
        Behaviors.same
    }
  }


  def execStatementsPlan(stmts: Seq[String], effectProps: EffectiveExecConfig)(implicit ctx: ActorContext[Command]): Done = {
    // create flink context
    val flinkCtx = createFlinkContext(effectProps.flinkConfig)

    // parse sql statements and execute non-immediate operations
    val stashToken = StashOpToken()
    breakable {
      for (stmt <- stmts) {
        if (stashToken.queryOp.isDefined) break
        // parse statement
        val op = Try(flinkCtx.parser.parse(stmt).get(0)).foldIdentity { err =>
          err.printStackTrace()
          return Done
        }
        op match {
          case op: QueryOperation => stashToken.queryOp = Some(stmt -> op)
          case op: ModifyOperation => stashToken.modifyOps += stmt -> op
          case op =>
            // when a ModifyOperation has been staged, the remaining normal statement would be skipped.
            if (stashToken.modifyOps.nonEmpty) break
            val tableResult: TableResult = Try(flinkCtx.tEnvInternal.executeInternal(op)).foldIdentity { err =>
              err.printStackTrace()
              return Done
            }
            // collect result from flink TableResult immediately
            val cols = FlinkApiCovertTool.extractSchema(tableResult)
            val rows = Using(tableResult.collect)(iter => iter.asScala.map(row => FlinkApiCovertTool.covertRow(row)).toSeq)
              .foldIdentity { err =>
                err.printStackTrace()
                return Done
              }
            // todo
            println(TableResultData(cols, rows).tabulateContent)
        }
      }
    }

    if (stashToken.isEmpty) return Done
    // execute stashed operations
    stashToken.toEither match {
      case Right(stashModifyOps) =>
        val (stmts, modifyOps) = stashModifyOps.map(_._1).mkString(";") -> stashModifyOps.map(_._2)
        Try(flinkCtx.tEnvInternal.executeInternal(modifyOps.asJava)) match {
          case Failure(err) =>
            err.printStackTrace()
            Done
          case Success(tableResult) =>
            val jobId: Option[String] = tableResult.getJobClient.asScala.map(_.getJobID.toString)
            println(s"Submit modify statements to Flink cluster successfully, jobId = $jobId") // todo
            Done
        }

      case Left((stmt, queryOp)) =>
        Try(flinkCtx.tEnvInternal.executeInternal(queryOp)) match {
          case Failure(err) =>
            err.printStackTrace()
            Done
          case Success(tableResult) =>
            val jobId: Option[String] = tableResult.getJobClient.asScala.map(_.getJobID.toString)
            println(s"Submit query statement to Flink cluster successfully, jobId = $jobId") // todo
            val cols = FlinkApiCovertTool.extractSchema(tableResult)
            println(s"cols: $cols") // todo
            Using(tableResult.collect) { iter =>
              effectProps.resultCollectStrategy match {
                case RsCollectStrategy(EvictStrategy.DROP_TAIL, limit) =>
                  iter.asScala.take(limit).foreach(row => println(s"row: ${FlinkApiCovertTool.covertRow(row)}")) // todo
                case _ =>
                  iter.asScala.foreach(row => println(s"row: ${FlinkApiCovertTool.covertRow(row)}")) // todo
              }
            } match {
              case Failure(err) =>
                err.printStackTrace()
                Done
              case Success(_) =>
                println("collect result finished")
                Done
            }
        }
    }

  }


  /**
   * Flink environment context.
   */
  case class FlinkContext(tEnv: StreamTableEnvironment, tEnvInternal: TableEnvironmentInternal, parser: Parser)

  /**
   * Initialize flink context from ExecConfig
   */
  def createFlinkContext(flinkConfig: Map[String, String]): FlinkContext = {
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
  case class StashOpToken(var queryOp: Option[(String, QueryOperation)] = None,
                          modifyOps: mutable.Buffer[(String, ModifyOperation)] = mutable.Buffer.empty) {

    def isEmpty: Boolean = queryOp.isEmpty && modifyOps.isEmpty
    def toEither: Either[(String, QueryOperation), Seq[(String, ModifyOperation)]] =
      if (queryOp.isDefined) Left(queryOp.get) else Right(modifyOps)
  }

  /**
   * Flink sql statements execution result buffer.
   */
  case class StmtsRsBuffer(result: mutable.Seq[SingleStmtResult], startTs: Long, lastTs: Long)

  type DataRowBuffer = AbstractSeq[RowData] with mutable.Builder[RowData, AbstractSeq[RowData]]

  case class ResultBuffer(opType: TrackOpType,
                          statement: String,
                          var jobId: Option[String] = None,
                          var cols: Seq[Column] = Seq.empty,
                          rows: DataRowBuffer,
                          var error: Option[Error] = None,
                          var isFinished: Boolean = false,
                          startTs: Long,
                          var ts: Long = curTs)
}
