package com.github.potamois.potamoi.flinkgateway

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.operations.{Operation, QueryOperation}

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
 * @author Al-assad
 */
object InteractSqlService extends InteractServiceTrait {

  // todo refactor to akka actor or akka distributed data
  val sessionManager = mutable.Map.empty[String, SessionContext]

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
    sessionManager.remove(sessionId)
  }

  override def setSessionProperties(sessionId: String, properties: Map[String, String]): Unit = ???

  override def getSessionProperties(sessionId: String): Map[String, String] = ???

  override def setSessionResultCollectStrategy(sessionId: String, resultCollectStrategy: ResultCollectStrategy): Unit = ???

  override def getSessionResultCollectStrategy(sessionId: String): ResultCollectStrategy = ???

  override def getSessionResultExecStates(sessionId: String): SafeResult[Map[InteractSqlService.ResultId, ResultDescriptorState]] = ???



  override def parseStatement(sessionId: String, statement: String): SafeResult[Operation] = ???

  override def executeOperation(sessionId: String, ops: Operation): SafeResult[TableResultData] = ???

  override def executeModifyOperations(sessionId: String, modifyOps: Seq[Operation]): SafeResult[ResultDescriptor] = ???

  override def executeQueryOperation(sessionId: String, queryOps: QueryOperation): SafeResult[ResultDescriptor] = ???

  override def cancelQuery(sessionId: String, resultId: String): SafeResult[Unit] = ???

  override def retrieveModifyResult(sessionId: String, resultId: String): SafeResult[ModifyResult] = ???

  override def retrieveQueryResult(sessionId: String, resultId: String): SafeResult[QueryResult] = ???

  override def receiveResultRow(sessionId: String, resultId: String, handle: ResultRowData => Unit): SafeResult[Unit] = ???


}
