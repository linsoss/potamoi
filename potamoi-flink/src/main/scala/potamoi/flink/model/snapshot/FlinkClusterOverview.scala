package potamoi.flink.model.snapshot

import potamoi.KryoSerializable
import potamoi.flink.model.{Fcid, FlinkTargetType}
import potamoi.flink.model.FlinkTargetTypes.given
import zio.json.JsonCodec

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
