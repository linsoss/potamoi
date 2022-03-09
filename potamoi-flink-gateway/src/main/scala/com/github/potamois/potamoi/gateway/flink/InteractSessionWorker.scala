package com.github.potamois.potamoi.gateway.flink

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.github.potamois.potamoi.commons.TryImplicits.{TryWrapper, Wrapper}
import com.github.potamois.potamoi.commons.Using
import com.github.potamois.potamoi.gateway.flink.InteractSessionWorker.OpType.OpType
import com.github.potamois.potamoi.gateway.flink.InteractSessionWorker._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableResult
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.delegation.Parser
import org.apache.flink.table.operations.{ModifyOperation, QueryOperation}

import scala.collection.JavaConverters._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
 * todo use standalone dispatcher
 *
 * @author Al-assad
 */
object InteractSessionWorker {

  type ExecResult = Either[Error, TraceableExecResult]

  sealed trait Command
  // todo get and set context properties
  // execute queries
  final case class ExecuteQuery(sqlStatement: String, replyTo: ActorRef[ExecResult]) extends Command

  private sealed trait Internal extends Command
  private case class CollectModifyOperationResult(result: Try[TableResult]) extends Internal

  def apply(sessionId: String, props: ExecConfig): Behavior[Command] = Behaviors.setup { ctx =>
    new InteractSessionWorker(sessionId, props).action()
  }


  case class FlinkContext(tEnv: StreamTableEnvironment, tEnvInternal: TableEnvironmentInternal, parser: Parser)

  object FlinkContext {
    def initFrom(props: ExecConfig): FlinkContext = {
      val config = Configuration.fromMap(props.flinkConfig.asJava)
      val env = new StreamExecutionEnvironment(config)
      val tEnv = StreamTableEnvironment.create(env)
      val tEnvInternal = tEnv.asInstanceOf[TableEnvironmentInternal]
      val parser = tEnvInternal.getParser
      FlinkContext(tEnv, tEnvInternal, parser)
    }
  }

  case class OpProcessSignal(opType: OpType, sqlStatement: String, startSt: Long) extends Command

  object OpType extends Enumeration {
    type OpType = Value
    val QUERY, MODIFY = Value
  }
}


class InteractSessionWorker(sessionId: String, props: ExecConfig) {

  private val flinkCtx = FlinkContext.initFrom(props)

  private var inProcess: Option[OpProcessSignal] = None

  def action(): Behavior[Command] = Behaviors.receiveMessage {
    case ExecuteQuery(statement, replyTo) =>
      // parse sql statement
      val operation = Try(flinkCtx.parser.parse(statement).get(0)) match {
        case Success(result) => result
        case Failure(cause) =>
          replyTo ! Left(Error(s"Fail to parse the sql: $statement", cause))
          return Behaviors.same
      }

      // execute the operation
      operation match {
        case ModifyOperation =>
          // todo sign control
          inProcess = Some(OpProcessSignal(OpType.MODIFY, statement, System.currentTimeMillis))
          replyTo ! Right(SubmitModifyOpDone)

        case QueryOperation =>
          // todo sign control
          inProcess = Some(OpProcessSignal(OpType.QUERY, statement, System.currentTimeMillis))
          replyTo ! Right(SubmitQueryOpDone)

        case op =>
          val tableResult: TableResult = Try(flinkCtx.tEnvInternal.executeInternal(op)).foldIdentity { err =>
            replyTo ! Left(Error(s"Fail to execute the sql: $statement", err))
            return Behaviors.same
          }
          val cols = FlinkApiCovertTool.extractSchema(tableResult)
          val rows = Using(tableResult.collect()) { iter =>
            iter.asScala.map(row => FlinkApiCovertTool.covertRow(row)).toSeq
          }.foldIdentity { err =>
            replyTo ! Left(Error(s"Fail to collect the result: $statement", err))
            return Behaviors.same
          }

          replyTo ! Right(ImmediateExecDone(TableResultData(cols, rows)))
      }
      Behaviors.same
  }


}
