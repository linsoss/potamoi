package potamoi.flink.interp.model

import io.circe.Json
import org.apache.flink.table.api.ResultKind
import org.apache.flink.table.types.logical.LogicalTypeRoot
import org.apache.flink.types.RowKind
import potamoi.flink.interp.SqlExecutor.HandleId

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Flink table execution result.
 */
case class SqlRsView(
    kind: ResultKind,
    columns: List[FieldMeta] = List.empty,
    data: ListBuffer[RowValue] = ListBuffer.empty)

object SqlRsView:

  lazy val plainOkResult: SqlRsView = SqlRsView(
    kind = ResultKind.SUCCESS,
    columns = List(
      FieldMeta(
        name = "result",
        typeRoot = LogicalTypeRoot.VARCHAR,
        typeDesc = "STRING"
      )),
    data = ListBuffer(
      RowValue(
        kind = RowKind.INSERT,
        fields = Json.fromValues(Seq(Json.fromString("OK")))
      ))
  )

end SqlRsView

/**
 * See [[org.apache.flink.table.catalog.Column]]
 */
case class FieldMeta(
    name: String,
    typeRoot: LogicalTypeRoot,
    typeDesc: String,
    comment: Option[String] = None)

/**
 * See: [[org.apache.flink.types.Row]]
 *
 * The row data will be encoded as a json structure, for the corresponding mapping
 * refer to: [[ https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/formats/json/#data-type-mapping ]]
 */
case class RowValue(kind: RowKind, fields: Json)
