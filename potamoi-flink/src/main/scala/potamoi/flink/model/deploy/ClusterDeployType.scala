package potamoi.flink.model.deploy

import potamoi.codecs
import zio.json.JsonCodec

/**
 * Flink cluster deployment type.
 */
enum ClusterDeployType:
  case Session, JarApp, SqlApp

object ClusterDeployTypes:
  given JsonCodec[ClusterDeployType] = codecs.simpleEnumJsonCodec(ClusterDeployType.values)
