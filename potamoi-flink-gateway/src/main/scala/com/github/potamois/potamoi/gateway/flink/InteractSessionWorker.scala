package com.github.potamois.potamoi.gateway.flink

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.github.potamois.potamoi.commons.Using
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableResult
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.delegation.Parser
import org.apache.flink.table.operations.QueryOperation

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success, Try}

/**
 * todo use standalone dispatcher
 * @author Al-assad
 */
object InteractSessionWorker {

  type SqlParseResult = Either[Error, TableResultData]

  sealed trait Command
  // todo get and set context properties

  // execute queries
  final case class ExecuteQuery(statement: String, replyTo: ActorRef[SqlParseResult]) extends Command


  private sealed trait Internal extends Command
  private case class CollectModifyOperationResult(result: Try[TableResult]) extends Internal

  //  private final case class ExecuteQueryOperation(ops: QueryOperation) extends Internal


  case class SessionContext(tEnv: StreamTableEnvironment, tEnvInternal: TableEnvironmentInternal, parser: Parser)

  object SessionContext {
    /**
     * Create a SessionContext instance from [[ExecConfig]].
     */
    def initFrom(props: ExecConfig): SessionContext = {
      // initialize flink TableEnvironment
      val tEnv = {
        val config = Configuration.fromMap(props.flinkConfig.asJava)
        val env = new StreamExecutionEnvironment(config)
        StreamTableEnvironment.create(env)
      }
      val tEnvInternal = tEnv.asInstanceOf[TableEnvironmentInternal]
      val parser = tEnvInternal.getParser
      SessionContext(tEnv, tEnvInternal, parser)
    }

  }


  def apply(sessionId: String, props: ExecConfig): Behavior[Command] = Behaviors.setup { ctx =>

    // todo replace standalone dispatcher
    implicit val ec: ExecutionContextExecutor = ctx.system.executionContext

    val context = SessionContext.initFrom(props)

    Behaviors.receiveMessage {

      case ExecuteQuery(statement, replyTo) =>
        // parse sql statement
        val operation = Try(context.parser.parse(statement).get(0)) match {
          case Success(result) => result
          case Failure(cause) =>
            replyTo ! Left(Error(s"Fail to parse the sql: $statement", cause))
            return Behaviors.same
        }

        // execute the operation
        operation match {
          // case op: ModifyOperation =>
          case op: QueryOperation =>

          case op =>
            val tableResult: TableResult = Try(context.tEnvInternal.executeInternal(op)) match {
              case Success(result) => result
              case Failure(cause) =>
                replyTo ! Left(Error(s"Fail to execute the sql: $statement", cause))
                return Behaviors.same
            }
            val cols = FlinkApiCovertTool.extractSchema(tableResult)
            val rows = Using(tableResult.collect()) { iter =>
              iter.asScala.map(row => FlinkApiCovertTool.covertRow(row)).toSeq
            } match {
              case Success(result) => result
              case Failure(cause) =>
                replyTo ! Left(Error(s"Fail to collect the result: $statement", cause))
                return Behaviors.same
            }
            TableResultData(cols, rows)
        }
        Behaviors.same


    }
  }

}

