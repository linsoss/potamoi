package potamoi.flink.model

import potamoi.flink.model.FlinkExecModes.given_JsonCodec_FlinkExecMode
import zio.json.JsonCodec

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
    ts: Long)
    derives JsonCodec:
  lazy val fcid = Fcid(clusterId, namespace)

case class JobsStats(
    running: Int,
    finished: Int,
    canceled: Int,
    failed: Int)
    derives JsonCodec
