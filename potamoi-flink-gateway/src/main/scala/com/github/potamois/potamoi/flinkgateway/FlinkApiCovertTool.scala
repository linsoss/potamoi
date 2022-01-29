package com.github.potamois.potamoi.flinkgateway

import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

/**
 * @author Al-assad
 */
object FlinkApiCovertTool {


  /**
   * Convert Flink [[ResolvedSchema]] to Potamoi [[Column]] sequence.
   *
   * @param schema should not be null
   */
  def covertResolvedSchema(schema: ResolvedSchema): Seq[Column] = {
    schema.getColumns.asScala.map(col =>
      if (col == null) Column("", "")
      else Column(col.getName, col.getDataType.toString))
  }

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
