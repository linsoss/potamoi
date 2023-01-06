package potamoi.flink.model

import potamoi.codecs
import zio.json.JsonCodec

/**
 * Flink execution runtime mode.
 *
 * see: https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/config/#execution-runtime-mode
 */
enum FlinkRuntimeMode(val rawValue: String):
  case Streaming extends FlinkRuntimeMode("STREAMING")
  case Batch     extends FlinkRuntimeMode("BATCH")
  case Automatic extends FlinkRuntimeMode("AUTOMATIC")

object FlinkRuntimeModes:
  given JsonCodec[FlinkRuntimeMode] = codecs.simpleEnumJsonCodec(FlinkRuntimeMode.values)
