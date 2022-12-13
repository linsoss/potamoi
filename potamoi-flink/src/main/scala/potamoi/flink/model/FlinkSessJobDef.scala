package potamoi.flink.model

import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Definition of the job submitted to Flink session cluster.
 */
case class FlinkSessJobDef(
    clusterId: String,
    namespace: String,
    jobJar: String,
    appMain: Option[String] = None,
    appArgs: List[String] = List.empty,
    parallelism: Option[Int] = None,
    savepointRestore: Option[SavepointRestoreConfig] = None)

object FlinkSessJobDef:
  given JsonCodec[FlinkSessJobDef] = DeriveJsonCodec.gen[FlinkSessJobDef]
