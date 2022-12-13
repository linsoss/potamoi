package potamoi.flink.model

import zio.json.{DeriveJsonCodec, JsonCodec, JsonDecoder, JsonEncoder}

/**
 * Flink job savepoint conf.
 */
case class FlinkJobSavepointDef(
    drain: Boolean = false,
    savepointPath: Option[String] = None,
    formatType: Option[SavepointFormatType] = None,
    triggerId: Option[String] = None)

enum SavepointFormatType(val value: String):
  case Canonical extends SavepointFormatType("CANONICAL")
  case Native    extends SavepointFormatType("NATIVE")

object SavepointFormatTypes:
  given JsonCodec[SavepointFormatType] = JsonCodec(
    JsonEncoder[String].contramap(_.value),
    JsonDecoder[String].mapOrFail(s => SavepointFormatType.values.find(_.value == s).toRight(s"Unknown savepoint format type: $s"))
  )

object FlinkJobSavepointDef:
  import SavepointFormatTypes.given
  given JsonCodec[FlinkJobSavepointDef] = DeriveJsonCodec.gen[FlinkJobSavepointDef]
