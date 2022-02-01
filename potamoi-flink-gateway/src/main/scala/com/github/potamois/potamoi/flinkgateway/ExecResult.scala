package com.github.potamois.potamoi.flinkgateway

import com.github.potamois.potamoi.commons.Tabulator
import com.github.potamois.potamoi.flinkgateway.TrackOpsType.TrackOpsType

import scala.collection.SortedMap

case class ExecResult(sqlPlan: SortedMap[Int, String],
                      resultData: SortedMap[Int, TableResultData] = SortedMap.empty,
                      error: Option[(Int, Error)],
                      trackOps: TrackOpsType = TrackOpsType.NONE,
                      trackOpsId: Option[String] = None) {

  def needTrack: Boolean = TrackOpsType.NONE != trackOps
  def isValid: Boolean = error.isEmpty
}

/**
 * The type of operation for which the trace result is required.
 *
 * 1) NONE: The result does not need to be tracked;
 * 2) QUERY: Tracking of query results caused by statements like "select ...";
 * 3) MODIFY: Tracking of modify statements like "insert ...";
 *
 * @author Al-assad
 */
object TrackOpsType extends Enumeration {
  type TrackOpsType = Value
  val NONE, QUERY, MODIFY = Value
}

case class QueryResult(trackOpsId: String,
                       jobId: String,
                       isEnded: Boolean,
                       rowCount: Long,
                       error: Option[Error],
                       resultData: TableResultData,
                       lastTs: Long) {
  def hasErr: Boolean = error.isDefined
}

case class QueryResultPageSnapshot(result: QueryResult, page: PageRsp)

case class ModifyResult(trackOpsId: String,
                        jobId: String,
                        isEnded: Boolean,
                        error: Option[Error],
                        resultData: TableResultData,
                        ts: Long) {
  def hasErr: Boolean = error.isDefined
}

/**
 * Data records of a table that extracted from [[org.apache.flink.table.api.TableResult]].
 *
 * @param cols meta info of columns
 * @param data rows data
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

