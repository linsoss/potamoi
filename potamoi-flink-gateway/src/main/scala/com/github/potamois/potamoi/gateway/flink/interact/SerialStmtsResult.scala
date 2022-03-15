package com.github.potamois.potamoi.gateway.flink.interact

import com.github.potamois.potamoi.commons.curTs
import com.github.potamois.potamoi.gateway.flink.Error
import com.github.potamois.potamoi.gateway.flink.interact.OpType.OpType

/**
 * Flink sqls serial execution result.
 *
 * @param result     sequence of statement execution result
 * @param isFinished whether the current execution plan is finished
 * @param lastOpType type of last operation, see [[OpType]]
 * @param startTs    start timestamp
 * @param lastTs     state last updated timestamp
 * @author Al-assad
 */
case class SerialStmtsResult(result: Seq[SingleStmtResult], isFinished: Boolean, lastOpType: OpType, startTs: Long, lastTs: Long)

/**
 * Single sql statement result for [[SerialStmtsResult]]
 *
 * @param stmt sql statement
 * @param rs   flink TableResult data for this statement
 * @param ts   result received timestamp
 */
case class SingleStmtResult(stmt: String, rs: Either[Error, OperationDone], ts: Long) {

  // get operation type of this statement result.
  def opType: OpType = rs match {
    case Left(_) => OpType.UNKNOWN
    case Right(done) => done match {
      case _: ImmediateOpDone => OpType.NORMAL
      case _: SubmitModifyOpDone => OpType.MODIFY
      case _: SubmitQueryOpDone => OpType.QUERY
    }
  }
}

object SingleStmtResult {
  def fail(stmt: String, error: Error): SingleStmtResult = SingleStmtResult(stmt, Left(error), curTs)
  def success(stmt: String, rs: OperationDone): SingleStmtResult = SingleStmtResult(stmt, Right(rs), curTs)
}


/**
 * Flink sql operation type.
 *
 * 1) NORMAL: normal sql statement that executed in local, like "create ..., explain...";
 * 2) QUERY: query sql statement that executed in flink cluster, like "select ...";
 * 3) MODIFY: modify sql statement, like "insert ...";
 *
 * @author Al-assad
 */
object OpType extends Enumeration {
  type OpType = Value
  val UNKNOWN, NORMAL, QUERY, MODIFY = Value
}


