package com.github.potamois.potamoi.gateway.flink.interact

import akka.Done
import akka.actor.typed.ActorRef
import com.github.potamois.potamoi.commons.CborSerializable
import com.github.potamois.potamoi.gateway.flink.PageReq

/**
 * Flink sql interaction executor.
 * The default implementation is [[FsiSerialExecutor]].
 *
 * @author Al-assad
 */
object FsiExecutor {

  type RejectableDone = Either[ExecReqReject, Done]
  type ExecutionPlanResult = Option[SerialStmtsResult]
  type QueryResult = Option[TableResultSnapshot]
  type PageQueryResult = Option[PageableTableResultSnapshot]

  trait Command extends CborSerializable

  /**
   * Execute a new sql plan.
   *
   * @param sqlStatements sql statements
   * @param props         execution configuration
   */
  final case class ExecuteSqls(sqlStatements: String, props: ExecConfig, replyTo: ActorRef[RejectableDone]) extends Command

  /**
   * Check if the current executor is executing the sql plan.
   */
  final case class IsInProcess(replyTo: ActorRef[Boolean]) extends Command

  /**
   * Cancel the execution plan in process.
   */
  final case object CancelCurProcess extends Command

  /**
   * Terminate the executor.
   */
  final case class Terminate(reason: String = "") extends Command

  /**
   * Subscribe the result change events from this executor, see [[ResultChange]].
   */
  final case class SubscribeState(listener: ActorRef[ResultChange]) extends Command

  /**
   * Unsubscribe the result change events from this executor.
   */
  final case class UnsubscribeState(listener: ActorRef[ResultChange]) extends Command


  sealed trait GetQueryResult extends Command

  /**
   * Get the current sqls plan result snapshot that has been executed.
   */
  final case class GetExecPlanRsSnapshot(replyTo: ActorRef[ExecutionPlanResult]) extends GetQueryResult
  val GetExecPlanResult: GetExecPlanRsSnapshot.type = GetExecPlanRsSnapshot

  /**
   * Get the TableResult snapshot that has been collected for QueryOperation.
   *
   * @param limit result size limit, -1 and Int.MaxValue means no limit
   */
  final case class GetQueryRsSnapshot(limit: Int = -1, replyTo: ActorRef[QueryResult]) extends GetQueryResult
  val GetQueryResult: GetQueryRsSnapshot.type = GetQueryRsSnapshot

  /**
   * Get the current sqls plan result snapshot that has been executed.
   *
   * @param page page request param, see[[PageReq]]
   */
  final case class GetQueryRsSnapshotByPage(page: PageReq, replyTo: ActorRef[PageQueryResult]) extends GetQueryResult
  val GetQueryResultByPage: GetQueryRsSnapshotByPage.type = GetQueryRsSnapshotByPage

}

