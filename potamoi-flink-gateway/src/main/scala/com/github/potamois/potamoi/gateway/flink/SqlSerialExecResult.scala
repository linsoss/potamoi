package com.github.potamois.potamoi.gateway.flink

import com.github.potamois.potamoi.commons.{CborSerializable, Tabulator, curTs}
import com.github.potamois.potamoi.gateway.flink.TrackOpType.TrackOpType

/**
 * Flink sqls serial execution result.
 *
 * @param result  flink TableResult data
 * @param trackOp see [[TrackOpType]]
 * @param startTs start timestamp
 * @param endTs   end timestamp
 * @author Al-assad
 */
case class SerialStmtsResult(result: Seq[SingleStmtResult], trackOp: TrackOpType, startTs: Long, endTs: Long)

/**
 * Single sql statement result for [[SerialStmtsResult]]
 *
 * @param stmt sql statement
 * @param rs   flink TableResult data for this statement
 * @param ts   result received timestamp
 */
case class SingleStmtResult(stmt: String, rs: Either[Error, TrackableExecRs], ts: Long)

object SingleStmtResult {
  def fail(stmt: String, error: Error): SingleStmtResult = SingleStmtResult(stmt, Left(error), curTs)
  def success(stmt: String, rs: TrackableExecRs): SingleStmtResult = SingleStmtResult(stmt, Right(rs), curTs)
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
 * Flink sql serial traceable execution result trait
 */
trait TrackableExecRs extends CborSerializable

/**
 * flink sql immediate operation execution result like "create ...","explain ..."
 */
case class ImmediateOpDone(data: TableResultData) extends TrackableExecRs

/**
 * submit flink modify operation(like "insert...") done
 */
case object SubmitModifyOpDone extends TrackableExecRs

/**
 * submit flink query operation(like "select...") done
 */
case object SubmitQueryOpDone extends TrackableExecRs


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
 * Data records of a table that extracted from [[org.apache.flink.table.api.TableResult]].
 *
 * @param cols meta info of columns, see [[Column]]
 * @param data rows data, see[[RowData]]
 * @author Al-assad
 */
case class TableResultData(cols: Seq[Column], data: Seq[RowData]) {

  /** Formatted as tabulated content string for console-like output */
  def tabulateContent: String = Tabulator.format(Seq(cols, data))
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
case class RowData(kind: String, values: Seq[String])

object RowData {
  def empty: RowData = RowData("", Seq.empty)
}


