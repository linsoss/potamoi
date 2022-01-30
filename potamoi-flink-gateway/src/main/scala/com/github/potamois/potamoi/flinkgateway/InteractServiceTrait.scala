package com.github.potamois.potamoi.flinkgateway

import com.github.potamois.potamoi.flinkgateway.ResultKind.ResultKind
import com.github.potamois.potamoi.flinkgateway.ResultState.ResultState
import org.apache.flink.table.operations.{ModifyOperation, Operation, QueryOperation}

/**
 * todo Is it necessary to split this trait into more responsibility-defined traits ?
 *
 * @author Al-assad
 */
trait InteractServiceTrait {

  type SessionId = String
  type ResultId = String

  // session manager api

  def openSession(sessionId: String): SafeResult[String] = openSession(sessionId, SessionContextConfig.default)

  def openSession(sessionId: String, contextConfig: SessionContextConfig): SafeResult[String]

  def closeSession(sessionId: String): Unit

  def setSessionProperties(sessionId: String, properties: Map[String, String]): Unit

  def getSessionProperties(sessionId: String): Map[String, String]

  def setSessionResultCollectStrategy(sessionId: String, resultCollectStrategy: ResultCollectStrategy): Unit

  def getSessionResultCollectStrategy(sessionId: String): ResultCollectStrategy

  def getSessionResultExecStates(sessionId: String): SafeResult[Map[ResultId, ResultDescriptorState]]

  // sql execution api
  def parseStatement(sessionId: String, statement: String): SafeResult[Operation]

  def executeOperation(sessionId: String, ops: Operation): SafeResult[TableResultData]

  def executeModifyOperations(sessionId: String, modifyOps: Seq[ModifyOperation]): SafeResult[ResultDescriptor]

  def executeQueryOperation(sessionId: String, queryOps: QueryOperation): SafeResult[ResultDescriptor]

  def cancelQuery(sessionId: String, resultId: String): SafeResult[Unit]

  // retrieve result api

  def retrieveModifyResult(sessionId: String, resultId: String): SafeResult[ModifyResult]

  def retrieveQueryResult(sessionId: String, resultId: String): SafeResult[QueryResult]

  def receiveResultRow(sessionId: String, resultId: String, handle: ResultRowData => Unit): SafeResult[Unit]

}

case class ResultDescriptor(sessionId: String, resultId: String, kind: ResultKind, contextConfig: SessionContextConfig)

case class ResultDescriptorState(descriptor: ResultDescriptor,
                                 state: ResultState,
                                 error: Option[Error] = None,
                                 ts: Long = System.currentTimeMillis)

case class TableResultData(cols: Seq[Column], data: Seq[RowData])

case class Column(name: String, dataType: String)

case class RowData(kind: String, values: Seq[String])

case class ModifyResult(jobId: String, result: TableResultData, st: Long = System.currentTimeMillis)

case class QueryResult(jobId: String, row: Long, result: TableResultData, st: Long = System.currentTimeMillis)

case class ResultRowData(jobId: String, cols: Seq[Column], var data: RowData, st: Long = System.currentTimeMillis)

object ResultKind extends Enumeration {
  type ResultKind = Value
  val MODIFY, QUERY = Value
}

object ResultState extends Enumeration {
  type ResultState = Value
  val PENDING, RUNNING, FINISHED, FAILED = Value
}
