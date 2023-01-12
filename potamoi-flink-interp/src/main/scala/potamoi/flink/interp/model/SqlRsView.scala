package potamoi.flink.interp.model

import io.circe.Json
import org.apache.flink.table.api.ResultKind
import org.apache.flink.table.types.logical.LogicalTypeRoot
import org.apache.flink.types.RowKind

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


