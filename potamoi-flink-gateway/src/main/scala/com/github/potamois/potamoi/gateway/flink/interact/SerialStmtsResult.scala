package com.github.potamois.potamoi.gateway.flink.interact

import com.github.nscala_time.time.Imports.richLong
import com.github.potamois.potamoi.commons.{RichString, RichThrowable, curTs}
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
case class SerialStmtsResult(result: Seq[SingleStmtResult], isFinished: Boolean, lastOpType: OpType, startTs: Long, lastTs: Long) {
  // whether all of the statements that has executed are success
  def allSuccess: Boolean = !result.exists(_.rs.isLeft)

  // format result content to string for debugging
  def toFriendlyString: String =
    s"""
       |SerialStmtsResult
       | => startFrom: ${startTs.toDateTime.toString("yyyy-MM-dd HH:mm:ss.SSS")},
       | => lastUpdated: ${lastTs.toDateTime.toString("yyyy-MM-dd HH:mm:ss.SSS")}
       | => isFinished: $isFinished
       |
       |""".stripMargin.concat(result.zipWithIndex.map(e => s"[${e._2 + 1}] => " + e._1.toFriendlyString).mkString("\n\n"))
}

/**
 * Single sql statement result for [[SerialStmtsResult]]
 *
 * @param stmt sql statement
 * @param rs   flink TableResult data for this statement
 * @param ts   result received timestamp
 */
case class SingleStmtResult(stmt: String, rs: Either[Error, OperationDone], ts: Long) {

  // get operation type of this statement result
  def opType: OpType = rs match {
    case Left(_) => OpType.UNKNOWN
    case Right(done) => done match {
      case _: ImmediateOpDone => OpType.NORMAL
      case _: SubmitModifyOpDone => OpType.MODIFY
      case _: SubmitQueryOpDone => OpType.QUERY
    }
  }

  // covert to printer friendly string
  def toFriendlyString: String = {
    val rsContent = rs match {
      case Left(err) => s"error => ${err.summary} \n${err.stack.getStackTraceAsString}"
      case Right(done) => done match {
        case r: SubmitModifyOpDone => r.toFriendlyString
        case r: SubmitQueryOpDone => r.toFriendlyString
        case r: ImmediateOpDone => r.data.tabulateContent(escapeJava = !stmt.trim.take(7).equalsIgnoreCase("explain"))
      }
    }
    s"""${stmt.compact}
       |type: $opType
       |ts: ${ts.toDateTime.toString("yyyy-MM-dd HH:mm:ss.SSS")}
       |""".stripMargin
      .concat(rsContent)
  }
}

object SingleStmtResult {
  def fail(stmt: String, error: Error): SingleStmtResult = SingleStmtResult(stmt, Left(error), curTs)
  def success(stmt: String, rs: OperationDone): SingleStmtResult = SingleStmtResult(stmt, Right(rs), curTs)
}
