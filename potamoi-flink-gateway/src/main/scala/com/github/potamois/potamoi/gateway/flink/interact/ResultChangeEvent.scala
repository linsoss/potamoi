package com.github.potamois.potamoi.gateway.flink.interact

import com.github.potamois.potamoi.commons.CborSerializable
import com.github.potamois.potamoi.gateway.flink.interact.OpType.OpType
import com.github.potamois.potamoi.gateway.flink.Error

/**
 * Execution result changes event for [[SqlSerialExecutor]] implemented by Akka Cluster Topic,
 * the subscribers can select to receive from the following list of [[ResultChangeEvent]] as
 * needed.
 *
 * There is a default implementation [[RsEventChangePrinter]], it can be used to receive events
 * in debugging scenario.
 *
 * @author Al-assad
 */
sealed trait ResultChange extends CborSerializable

object ResultChangeEvent {

  /**
   * Executor accepts a new sql statements execution plan request
   *
   * @param stmts  sql statements that has been split
   * @param config the effective execution configuration
   */
  final case class AcceptStmtsExecPlan(stmts: Seq[String], config: EffectiveExecConfig) extends ResultChange

  /**
   * Executor rejects a new sql statements execution plan request
   *
   * @param stmts sql statements
   * @param cause rejection reason
   */
  final case class RejectStmtsExecPlan(stmts: String, cause: ExecReqReject) extends ResultChange

  /**
   * A single sql statement begins to be executed.
   *
   * @param stmt sql statement
   */
  final case class SingleStmtStart(stmt: String) extends ResultChange

  /**
   * A single sql statement has been executed.
   *
   * @param rs execution result, may be error or TableResult data
   */
  final case class SingleStmtDone(rs: SingleStmtResult) extends ResultChange

  /**
   * All of the statements in the accepted execution plan has been executed.
   *
   * @param rs execution plan result
   */
  final case class AllStmtsDone(rs: SerialStmtsResult) extends ResultChange

  /**
   * A flink job is committed to a remote or local flink cluster during execution,
   * which happens in a statement of type ModifyOperation or QueryOperation.
   *
   * @param op      operation type
   * @param jobId   flink job id
   * @param jobName job name
   */
  final case class SubmitJobToFlinkCluster(op: OpType, jobId: String, jobName: String) extends ResultChange

  /**
   * Receive new TableResult columns meta information when QueryOperation is executed.
   *
   * @param columns TableResult columns
   */
  final case class ReceiveQueryOpColumns(columns: Seq[Column]) extends ResultChange

  /**
   * Receive a new single TableResult row when QueryOperation is executed.
   *
   * @param row TableResult row
   */
  final case class ReceiveQueryOpRow(row: Row) extends ResultChange

  /**
   * An error in the execution of QueryOperation often leads to early termination of
   * the entire execution plan.
   *
   * @param error error info
   */
  final case class ErrorDuringQueryOp(error: Error) extends ResultChange

  /**
   * The current execution plan is been cancelled.
   */
  final case object StmtsPlanExecCanceled extends ResultChange

  /**
   * The current executor is been terminated actively.
   */
  final case class ActivelyTerminated(reason: String) extends ResultChange


}
