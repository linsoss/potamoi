package com.github.potamois.potamoi.gateway.flink

import com.github.potamois.potamoi.commons.{CborSerializable, Tabulator, curTs}
import com.github.potamois.potamoi.gateway.flink.OpType.OpType
import com.github.potamois.potamoi.gateway.flink.TrackOpType.TrackOpType


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
 * Flink sql serial operation execution result trait
 */
trait OperationDone extends CborSerializable

/**
 * flink sql immediate operation execution result like "create ...","explain ..."
 */
case class ImmediateOpDone(data: TableResultData) extends OperationDone

/**
 * submit flink modify operation(like "insert...") done
 */
case class SubmitModifyOpDone(jobId: String) extends OperationDone {
  def toLog: String = s"Submit modify statement to Flink cluster successfully, jobId = $jobId"
}

/**
 * submit flink query operation(like "select...") done
 */
case class SubmitQueryOpDone(jobId: String) extends OperationDone {
  def toLog: String = s"Submit query statement to Flink cluster successfully, jobId = $jobId"
}


/**
 * Tracking statement result.
 *
 * @param opType     sql statement type, see[[TrackOpType]]
 * @param statement  sql statement
 * @param result     result data
 * @param jobId      flink job id for this statement
 * @param isFinished whether this statement is finished
 * @param startTs    start timestamp
 * @param ts         state updated timestamp
 * @author Al-assad
 */
case class TrackStmtResult(opType: TrackOpType, statement: String, result: Either[Error, TableResultData],
                           jobId: Option[String], isFinished: Boolean, startTs: Long, ts: Long)

/**
 * The type of operation for which the trace result is required.
 *
 * 1) NONE: The result does not need to be tracked;
 * 2) QUERY: Tracking of query results caused by statements like "select ...";
 * 3) MODIFY: Tracking of modify statements like "insert ...";
 *
 * @author Al-assad
 */
object TrackOpType extends Enumeration {
  type TrackOpType = Value
  val NONE, QUERY, MODIFY = Value
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

/**
 * Data records of a table that extracted from [[org.apache.flink.table.api.TableResult]].
 *
 * @param cols meta info of columns, see [[Column]]
 * @param rows rows data, see[[Row]]
 * @author Al-assad
 */
case class TableResultData(cols: Seq[Column], rows: Seq[Row]) {

  /** Formatted as tabulated content string for console-like output */
  def tabulateContent: String = Tabulator.format(Seq(cols, rows))
}

/**
 * Meta information of a column, which extract from [[org.apache.flink.table.api.TableColumn]].
 *
 * @param name     column name
 * @param dataType data type of this column
 * @author Al-assad
 */
case class Column(name: String, dataType: String)

/**
 * Record the content of a row of data, converted from [[org.apache.flink.types.Row]].
 *
 * @param kind   Short string of flink RowKind to describe the changelog type of a row,
 *               see [[org.apache.flink.types.RowKind]].
 * @param values All column values in a row of data will be converted to string, and
 *               the null value will be converted to "null".
 * @author Al-assad
 */
case class Row(kind: String, values: Seq[String])

object Row {
  def empty: Row = Row("", Seq.empty)
}


