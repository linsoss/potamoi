package potamoi.flink.model

import zio.json.JsonCodec
import FlinkTargetTypes.given_JsonCodec_FlinkTargetType
import potamoi.KryoSerializable

/**
 * Flink cluster overview.
 */
case class FlinkClusterOverview(
    clusterId: String,
    namespace: String,
    execType: FlinkTargetType,
    deployByPotamoi: Boolean,
    tmTotal: Int,
    slotsTotal: Int,
    slotsAvailable: Int,
    jobs: JobsStats,
    ts: Long)
    extends KryoSerializable
    derives JsonCodec:
  lazy val fcid = Fcid(clusterId, namespace)

case class JobsStats(
    running: Int,
    finished: Int,
    canceled: Int,
    failed: Int)
    derives JsonCodec
