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

case class TableResultData(cols: Seq[Column], data: Seq[RowData]) {
  /**
   * Formatted as tabulated content string
   */
  def tabulateContent: String = Tabulator.format(Seq(cols, data))
}

case class Column(name: String, dataType: String)

case class RowData(kind: String, values: Seq[String])

object RowData {
  def empty: RowData = RowData("", Seq.empty)
}

case class ModifyResult(trackOpsId: String,
                        jobId: String,
                        isEnded: Boolean,
                        error: Option[Error],
                        resultData: TableResultData,
                        ts: Long) {
  def hasErr: Boolean = error.isDefined
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


object TrackOpsType extends Enumeration {
  type TrackOpsType = Value
  val NONE, QUERY, MODIFY = Value
}
