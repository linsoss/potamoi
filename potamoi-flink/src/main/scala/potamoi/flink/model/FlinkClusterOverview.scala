package potamoi.flink.model

import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Flink cluster overview.
 */
case class FlinkClusterOverview(
    clusterId: String,
    namespace: String,
    execMode: FlinkExecMode,
    deployByPotamoi: Boolean,
    tmTotal: Int,
    slotsTotal: Int,
    slotsAvailable: Int,
    jobs: JobsStats,
    ts: Long):
  lazy val fcid = Fcid(clusterId, namespace)

case class JobsStats(
    running: Int,
    finished: Int,
    canceled: Int,
    failed: Int)

object FlinkClusterOverview:
  import potamoi.flink.model.FlinkExecModes.given
  given JsonCodec[JobsStats]            = DeriveJsonCodec.gen[JobsStats]
  given JsonCodec[FlinkClusterOverview] = DeriveJsonCodec.gen[FlinkClusterOverview]
  given Ordering[FlinkClusterOverview]  = Ordering.by(e => (e.clusterId, e.namespace))
