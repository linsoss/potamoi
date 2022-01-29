package com.github.potamois.potamoi.flinkgateway

import org.apache.flink.table.api.TableSchema
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

/**
 * @author Al-assad
 */

object FlinkApiCovertTool {

  /**
   * Covert Flink [[TableSchema]] to Potamoi [[TableSchema]].
   * Since ResolvedSchema is only available since flink-1.13, to maintain compatibility with the
   * deprecated TableSchema API.
   *
   * @note Please replace it with ResolvedSchema when the minimum flink version supported by potamoi
   *       is from 1.13.
   * @param schema should not be null
   */
  // noinspection ScalaDeprecation
  def covertTableSchema(schema: TableSchema): Seq[Column] = {
    schema.getTableColumns.asScala.map(col =>
      if (col == null) Column("", "")
      else Column(col.getName, col.getType.toString)
    )
  }

  /*  def covertResolvedSchema(schema: ResolvedSchema): Seq[Column] = {
    schema.getColumns.asScala.map(col =>
      if (col == null) Column("", "")
      else Column(col.getName, col.getDataType.toString))
  }*/

  /**
   * Covert Flink [[Row]] to Potamoi [[RowData]].
   *
   * @param row should not be null
   */
  def covertRow(row: Row): RowData = {
    val rowDataValues = (0 until row.getArity).map(i => if (row.getField(i) != null) row.getField(i).toString else "null")
    RowData(row.getKind.shortString(), rowDataValues)
  }


}
