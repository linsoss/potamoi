package potamoi.flink.interpreter.model

import io.circe.Json
import org.apache.flink.table.api.ResultKind
import org.apache.flink.table.types.logical.LogicalTypeRoot
import org.apache.flink.types.RowKind
import potamoi.flink.interpreter.FlinkInterpErr.ExecOperationErr
import zio.stream.Stream
import potamoi.syntax.toPrettyStr

/**
 * Flink table execution result.
 */
sealed trait SqlResult:
  def handleId: String

sealed trait SqlResultView:
  def handleId: String

case class PlainSqlRs(
    handleId: String,
    kind: ResultKind,
    columns: List[FieldMeta] = List.empty,
    data: List[RowValue] = List.empty)
    extends SqlResult with SqlResultView

case class QuerySqlRs(
    handleId: String,
    kind: ResultKind,
    columns: List[FieldMeta] = List.empty,
    dataWatchStream: Stream[ExecOperationErr, RowValue])
    extends SqlResult

case class QuerySqlRsDescriptor(
    handleId: String,
    kind: ResultKind,
    columns: List[FieldMeta] = List.empty)
    extends SqlResultView

case class SqlResultPage(
    totalPage: Int,
    pageIndex: Int,
    pageSize: Int,
    payload: PlainSqlRs)

object PlainSqlRs:

  def apply(rs: QuerySqlRsDescriptor, data: List[RowValue]): PlainSqlRs =
    PlainSqlRs(handleId = rs.handleId, kind = rs.kind, columns = rs.columns, data = data)

  def plainOkResult(handleId: String): PlainSqlRs = PlainSqlRs(
    handleId = handleId,
    kind = ResultKind.SUCCESS,
    columns = List(FieldMeta("result", LogicalTypeRoot.VARCHAR, "STRING")),
    data = List(RowValue(RowKind.INSERT, Json.fromValues(Seq(Json.fromString("OK")))))
  )

object QuerySqlRs:

  def apply(rs: QuerySqlRsDescriptor, dataStream: Stream[ExecOperationErr, RowValue]): QuerySqlRs =
    QuerySqlRs(rs.handleId, rs.kind, rs.columns, dataStream)

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
case class RowValue(kind: RowKind, fields: Json):
  def show: String = RowView(kind.shortString, fields.spaces2).toPrettyStr

private case class RowView(kind: String, fields: String)
