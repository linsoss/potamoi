package com.github.potamois.potamoi.gateway.flink.interact

import akka.Done
import akka.actor.typed.{ActorRef, Behavior}
import com.github.potamois.potamoi.akka.serialize.CborSerializable
import com.github.potamois.potamoi.gateway.flink.PageReq
import com.github.potamois.potamoi.gateway.flink.interact.FsiSessManager.SessionId

/**
 * Flink sql interaction executor.
 * The default implementation is [[FsiSerialExecutor]].
 *
 * @author Al-assad
 */
object FsiExecutor {

  type MaybeDone = Either[ExecReqReject, Done]
  type ExecPlanResult = Option[SerialStmtsResult]
  type QueryResult = Option[TableResultSnapshot]
  type PageQueryResult = Option[PageableTableResultSnapshot]

  sealed trait Command extends CborSerializable

  /**
   * Execute a new sql plan.
   *
   * @param sqlStatements sql statements
   * @param props         execution configuration properties
   */
  final case class ExecuteSqls(sqlStatements: String, props: ExecProps, replyTo: ActorRef[MaybeDone]) extends Command

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
   * Subscribe the result change events from this executor, see [[ExecRsChangeEvent]].
   */
  final case class SubscribeState(listener: ActorRef[ExecRsChangeEvent]) extends Command

  /**
   * Unsubscribe the result change events from this executor.
   */
  final case class UnsubscribeState(listener: ActorRef[ExecRsChangeEvent]) extends Command


  sealed trait GetQueryResult extends Command

  /**
   * Get the current sqls plan result snapshot that has been executed.
   */
  final case class GetExecPlanRsSnapshot(replyTo: ActorRef[ExecPlanResult]) extends GetQueryResult
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


  trait Expansion extends Command

  /**
   * Default behavior, see [[FsiSerialExecutor]]
   */
  def apply(sessionId: SessionId): Behavior[Command] = FsiSerialExecutor(sessionId)

}

