package potamoi.flink.model.deploy

import potamoi.codecs
import potamoi.flink.model.deploy.SavepointFormatType
import potamoi.flink.model.deploy.SavepointFormatTypes.given
import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}

/**
 * Flink job savepoint specification.
 */
case class JobSavepointSpec(
    drain: Boolean = false,
    savepointPath: Option[String] = None,
    formatType: Option[SavepointFormatType] = None,
    triggerId: Option[String] = None)
    derives JsonCodec

enum SavepointFormatType(val rawValue: String):
  case Canonical extends SavepointFormatType("CANONICAL")
  case Native    extends SavepointFormatType("NATIVE")

object SavepointFormatTypes:
  given JsonCodec[SavepointFormatType] = codecs.simpleEnumJsonCodec(SavepointFormatType.values)
