package com.github.potamois.potamoi.gateway.flink

import scala.collection.JavaConverters._

/**
 * Flink API conversion tool.
 *
 * @author Al-assad
 */
object FlinkApiCovertTool {


  /**
   * Extract schema information from Flink [[org.apache.flink.table.api.TableResult]].
   * see [[covertTableSchema]]
   */
  def extractSchema(tableResult: org.apache.flink.table.api.TableResult): Seq[Column] = covertTableSchema(tableResult.getTableSchema)

  /**
   * Covert Flink [[org.apache.flink.table.api.TableSchema]] to Potamoi [[Column]] sequence.
   * Since ResolvedSchema is only available since flink-1.13, to maintain compatibility with the
   * deprecated TableSchema API.
   *
   * @note Please replace it with ResolvedSchema when the minimum flink version supported by potamoi
   *       is from 1.13.
   */
  // noinspection ScalaDeprecation
  def covertTableSchema(schema: org.apache.flink.table.api.TableSchema): Seq[Column] = schema match {
    case null => Seq.empty
    case _ => schema.getTableColumns.asScala.map(col =>
      if (col == null) Column("", "")
      else Column(col.getName, col.getType.toString)
    )
  }

  /**
   * Covert Flink [[org.apache.flink.types.Row]] to Potamoi [[Row]].
   */
  def covertRow(row: org.apache.flink.types.Row): Row = row match {
    case null => Row.empty
    case _ =>
      val rowDataValues = (0 until row.getArity)
        .map(i => if (row.getField(i) != null) row.getField(i).toString else "null")
      Row(row.getKind.shortString(), rowDataValues)
  }


}
