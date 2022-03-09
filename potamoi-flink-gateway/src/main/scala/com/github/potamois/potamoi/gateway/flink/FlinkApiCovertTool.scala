package com.github.potamois.potamoi.gateway.flink

import org.apache.flink.table.api.TableSchema
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

/**
 * Flink API conversion tool.
 *
 * @author Al-assad
 */
object FlinkApiCovertTool {

  /**
   * Covert Flink [[TableSchema]] to Potamoi [[Column]] sequence.
   * Since ResolvedSchema is only available since flink-1.13, to maintain compatibility with the
   * deprecated TableSchema API.
   *
   * @note Please replace it with ResolvedSchema when the minimum flink version supported by potamoi
   *       is from 1.13.
   */
  // noinspection ScalaDeprecation
  def covertTableSchema(schema: TableSchema): Seq[Column] = schema match {
    case null => Seq.empty
    case _ => schema.getTableColumns.asScala.map(col =>
      if (col == null) Column("", "")
      else Column(col.getName, col.getType.toString)
    )
  }


  /**
   * Covert Flink [[Row]] to Potamoi [[RowData]].
   */
  def covertRow(row: Row): RowData = row match {
    case null => RowData.empty
    case _ =>
      val rowDataValues = (0 until row.getArity).map(i => if (row.getField(i) != null) row.getField(i).toString else "null")
      RowData(row.getKind.shortString(), rowDataValues)
  }


}
