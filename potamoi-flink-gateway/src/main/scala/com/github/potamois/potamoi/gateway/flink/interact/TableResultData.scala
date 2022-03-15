package com.github.potamois.potamoi.gateway.flink.interact

import com.github.potamois.potamoi.commons.Tabulator

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

