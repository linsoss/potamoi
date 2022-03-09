package com.github.potamois.potamoi.gateway.flink

import com.github.potamois.potamoi.akka.CborSerializable
import com.github.potamois.potamoi.commons.Tabulator


/**
 * @author Al-assad
 */
trait TraceableExecResult extends CborSerializable

case class ImmediateExecDone(data: TableResultData) extends TraceableExecResult

case object SubmitModifyOpDone extends TraceableExecResult

case object SubmitQueryOpDone extends TraceableExecResult


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


