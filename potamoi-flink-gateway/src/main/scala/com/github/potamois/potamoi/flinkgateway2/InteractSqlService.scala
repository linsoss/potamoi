package com.github.potamois.potamoi.flinkgateway2

import com.github.potamois.potamoi.commons.{Using, Uuid}
import com.github.potamois.potamoi.flinkgateway.FlinkApiCovertTool.{covertRow, covertTableSchema}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.operations.{ModifyOperation, Operation, QueryOperation}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
 * @author Al-assad
 */
object InteractSqlService extends InteractServiceTrait {

  // todo refactor to akka actor or akka distributed data
  val sessionManager = mutable.Map.empty[SessionId, SessionContext]
  val resultDescriptors = mutable.Map.empty[(SessionId, ResultId), ResultDescriptorState]
  val modifyResultStorage = mutable.Map.empty[(SessionId, ResultId), ModifyResult]
  val queryResultStorage = mutable.Map.empty[(SessionId, ResultId), QueryResult]


  protected def retrieve[T](sessionId: String)(func: SessionContext => SafeResult[T]): SafeResult[T] = {
    sessionManager.get(sessionId) match {
      case Some(sessionContext) => func(sessionContext)
      case None => SafeResult.fail(s"sessionId $sessionId not found")
    }
  }


  override def openSession(sessionId: String, contextConfig: SessionContextConfig): SafeResult[String] = {
    sessionManager.get(sessionId) match {
      case Some(context) =>
        SafeResult.fail(s"sessionId ${context.sessionId} already exists")
      case None =>
        // todo init StreamTableEnvironment
        val tableEnv = {
          val config = Configuration.fromMap(contextConfig.flinkConfig.asJava)
          val env = new StreamExecutionEnvironment(config)
          StreamTableEnvironment.create(env)
        }
        val context = SessionContext(sessionId, contextConfig, tableEnv)
        sessionManager.put(sessionId, context)
        SafeResult.pass(sessionId)
    }
  }

  override def closeSession(sessionId: String): Unit = {
    // todo cancel all queries of the session
    sessionManager.remove(sessionId)
  }

  override def setSessionProperties(sessionId: String, properties: Map[String, String]): Unit = ???

  override def getSessionProperties(sessionId: String): Map[String, String] = ???

  override def setSessionResultCollectStrategy(sessionId: String, resultCollectStrategy: ResultCollectStrategy): Unit = ???

  override def getSessionResultCollectStrategy(sessionId: String): ResultCollectStrategy = ???

  override def getSessionResultExecStates(sessionId: String): SafeResult[Map[InteractSqlService.ResultId, ResultDescriptorState]] = ???


  override def parseStatement(sessionId: String, statement: String): SafeResult[Operation] = retrieve(sessionId) { session =>
    val parser = session.tableEnv.asInstanceOf[TableEnvironmentInternal].getParser
    Try(parser.parse(statement).get(0)) match {
      case Failure(cause) => SafeResult.fail("parser sql statement failed", cause)
      case Success(ops) => SafeResult.pass(ops)
    }
  }


  // noinspection ScalaDeprecation
  override def executeOperation(sessionId: String, ops: Operation): SafeResult[TableResultData] = retrieve(sessionId) { session =>
    val tableEnvInternal = session.tableEnv.asInstanceOf[TableEnvironmentInternal]
    Try(tableEnvInternal.executeInternal(ops)) match {
      case Failure(cause) =>
        SafeResult.fail("execute sql statement failed", cause)
      case Success(tableResult) =>
        // extract columns meta from ResolvedSchema
        val cols: Seq[Column] = covertTableSchema(tableResult.getTableSchema)
        // execute Operation and collect TableResult to RowData sequence
        val rowDatas: Seq[RowData] = Using(tableResult.collect()) { iter =>
          iter.asScala.map(covertRow).toSeq
        } match {
          case Success(rows) => rows
          case Failure(cause) => return SafeResult.fail("collect table result failed", cause)
        }
        SafeResult.pass(TableResultData(cols, rowDatas))
    }
  }


  override def executeModifyOperations(sessionId: String,
                                       modifyOps: Seq[ModifyOperation]): SafeResult[ResultDescriptor] = retrieve(sessionId) { session =>
    val resultId = Uuid.genShortUUID
    val descriptor = ResultDescriptor(sessionId, resultId, ResultKind.MODIFY, session.config)
    resultDescriptors += (sessionId, resultId) -> ResultDescriptorState(descriptor, ResultState.PENDING)

    val tableEnvInternal = session.tableEnv.asInstanceOf[TableEnvironmentInternal]
    val f = Future {
      resultDescriptors += (sessionId, resultId) -> ResultDescriptorState(descriptor, ResultState.RUNNING)
      // execute and submit flink job
      val tableResult = Try(tableEnvInternal.executeInternal(modifyOps.asJava)) match {
        case Success(tableResult) => tableResult
        case Failure(cause) =>
          resultDescriptors += (sessionId, resultId) ->
            ResultDescriptorState(descriptor, ResultState.FAILED, Some(Error("execute sql statement failed", cause.getMessage)))
          return SafeResult.fail("execute sql statement failed", cause)
      }
      val jobId = tableResult.getJobClient.get.getJobID.toHexString
      val cols: Seq[Column] = covertTableSchema(tableResult.getTableSchema)
      modifyResultStorage += (sessionId, resultId) -> ModifyResult(jobId, TableResultData(cols, Seq.empty))
      // collect result
      val rowDatas = Using(tableResult.collect()) { iter =>
        iter.asScala.map(covertRow).toSeq
      } match {
        case Success(rows) =>
          modifyResultStorage += (sessionId, resultId) -> ModifyResult(jobId, TableResultData(cols, rows))
          resultDescriptors += (sessionId, resultId) -> ResultDescriptorState(descriptor, ResultState.FINISHED)
        case Failure(exception) =>
          resultDescriptors += (sessionId, resultId) -> ResultDescriptorState(descriptor, ResultState.FAILED)
      }
    }
    SafeResult.pass(descriptor)
  }


  override def executeQueryOperation(sessionId: String, queryOps: QueryOperation): SafeResult[ResultDescriptor] = ???

  override def cancelQuery(sessionId: String, resultId: String): SafeResult[Unit] = ???

  override def retrieveModifyResult(sessionId: String, resultId: String): SafeResult[ModifyResult] = ???

  override def retrieveQueryResult(sessionId: String, resultId: String): SafeResult[QueryResult] = ???

  override def receiveResultRow(sessionId: String, resultId: String, handle: ResultRowData => Unit): SafeResult[Unit] = ???


}
