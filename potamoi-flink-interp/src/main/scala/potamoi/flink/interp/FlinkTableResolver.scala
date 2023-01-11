package potamoi.flink.interp

import io.circe
import io.circe.Json
import org.apache.flink.formats.common.TimestampFormat
import org.apache.flink.formats.json.{JsonFormatOptions, RowDataToJsonConverters}
import org.apache.flink.formats.json.RowDataToJsonConverters.RowDataToJsonConverter
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeType
import org.apache.flink.table.catalog.{Column, ResolvedSchema}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.logical.LogicalType
import potamoi.flink.interp.model.{FieldMeta, RowValue}

import java.math.BigDecimal as JBigDecimal
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.RichOptional

object FlinkTableResolver:

  /**
   * Convert flink [[ResolvedSchema]] to list of [[FieldMeta]].
   */
  def convertResolvedSchema(schema: ResolvedSchema): List[FieldMeta] =
    schema.getColumns.asScala.map { col =>
      FieldMeta(
        name = col.getName,
        typeRoot = col.getDataType.getLogicalType.getTypeRoot,
        typeDesc = col.getDataType.getLogicalType.asSummaryString(),
        comment = col.getComment.toScala
      )
    }.toList

  /**
   * Flink table [[RowData]] converter for converting raw flink RowData to array json structure.
   *
   * See:
   * [[org.apache.flink.formats.json.RowDataToJsonConverters]]
   * [[org.apache.flink.table.gateway.rest.serde.ResultInfoJsonSerializer]]
   */
  class RowDataConverter(schema: ResolvedSchema):

    private val objectMapper     = ObjectMapper()
    private val toJsonConverters = RowDataToJsonConverters(TimestampFormat.ISO_8601, JsonFormatOptions.MapNullKeyMode.LITERAL, "");

    private val converters: Vector[RowDataToJsonConverter] =
      schema.getColumns.asScala
        .map(_.getDataType.getLogicalType)
        .map(toJsonConverters.createConverter)
        .toVector

    private val fieldGetters: Vector[RowData.FieldGetter] =
      schema.getColumns.asScala
        .map(_.getDataType.getLogicalType)
        .zipWithIndex
        .map { case (logicalType, idx) => RowData.createFieldGetter(logicalType, idx) }
        .toVector

    def convertRow(row: RowData) = {
      val arrayNode = objectMapper.createArrayNode()
      val jsonNodes = converters.zipWithIndex.map { case (conv, i) =>
        conv.convert(objectMapper, null, fieldGetters(i).getFieldOrNull(row))
      }
      arrayNode.addAll(jsonNodes.asJava)
      val rowData = jacksonToCirce(arrayNode)
      RowValue(row.getRowKind, rowData)
    }

  end RowDataConverter

  /**
   * Convert jackson [[JsonNode]] to circe [[Json]].
   */
  private def jacksonToCirce(node: JsonNode): circe.Json = {
    node.getNodeType match
      case JsonNodeType.BOOLEAN => Json.fromBoolean(node.asBoolean)
      case JsonNodeType.STRING  => Json.fromString(node.asText)
      case JsonNodeType.ARRAY   => Json.fromValues(node.elements.asScala.map(jacksonToCirce).toIndexedSeq)
      case JsonNodeType.OBJECT  => Json.fromFields(node.fields.asScala.map(m => (m.getKey, jacksonToCirce(m.getValue))).toIndexedSeq)
      case JsonNodeType.NUMBER =>
        if node.isFloatingPointNumber then Json.fromBigDecimal(JBigDecimal(node.asText)) else Json.fromBigInt(node.bigIntegerValue)
      case _ => Json.Null
  }

end FlinkTableResolver
