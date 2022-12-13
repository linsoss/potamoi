package potamoi.flink.model

import potamoi.curTs
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Flink job metrics.
 * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/metrics
 */
case class FlinkJobMetrics(
    clusterId: String,
    namespace: String,
    jobId: String,
    cancellingTime: Option[Long] = None,
    createdTime: Option[Long] = None,
    deployingTime: Option[Long] = None,
    downtime: Option[Long] = None,
    failingTime: Option[Long] = None,
    fullRestarts: Option[Long] = None,
    initializingTime: Option[Long] = None,
    lastCheckpointDuration: Option[Long] = None,
    lastCheckpointFullSize: Option[Long] = None,
    lastCheckpointPersistedData: Option[Long] = None,
    lastCheckpointProcessedData: Option[Long] = None,
    lastCheckpointRestoreTimestamp: Option[Long] = None,
    lastCheckpointSize: Option[Long] = None,
    numberOfCompletedCheckpoints: Option[Long] = None,
    numberOfFailedCheckpoints: Option[Long] = None,
    numberOfInProgressCheckpoints: Option[Long] = None,
    restartingTime: Option[Long] = None,
    runningTime: Option[Long] = None,
    totalNumberOfCheckpoints: Option[Long] = None,
    uptime: Option[Long] = None,
    ts: Long = curTs)

object FlinkJobMetrics:
  given JsonCodec[FlinkJobMetrics] = DeriveJsonCodec.gen[FlinkJobMetrics]

  val metricsRawKeys: Set[String] = Set(
    "cancellingTime",
    "createdTime",
    "deployingTime",
    "downtime",
    "failingTime",
    "fullRestarts",
    "initializingTime",
    "lastCheckpointDuration",
    "lastCheckpointFullSize",
    "lastCheckpointPersistedData",
    "lastCheckpointProcessedData",
    "lastCheckpointRestoreTimestamp",
    "lastCheckpointSize",
    "numberOfCompletedCheckpoints",
    "numberOfFailedCheckpoints",
    "numberOfInProgressCheckpoints",
    "restartingTime",
    "runningTime",
    "totalNumberOfCheckpoints",
    "uptime"
  )

  def fromRaw(fjid: Fjid, raw: Map[String, String]): FlinkJobMetrics = FlinkJobMetrics(
    clusterId = fjid.clusterId,
    namespace = fjid.namespace,
    jobId = fjid.jobId,
    cancellingTime = raw.get("cancellingTime").map(_.toLong),
    createdTime = raw.get("createdTime").map(_.toLong),
    deployingTime = raw.get("deployingTime").map(_.toLong),
    downtime = raw.get("downtime").map(_.toLong),
    failingTime = raw.get("failingTime").map(_.toLong),
    fullRestarts = raw.get("fullRestarts").map(_.toLong),
    initializingTime = raw.get("initializingTime").map(_.toLong),
    lastCheckpointDuration = raw.get("lastCheckpointDuration").map(_.toLong),
    lastCheckpointFullSize = raw.get("lastCheckpointFullSize").map(_.toLong),
    lastCheckpointPersistedData = raw.get("lastCheckpointPersistedData").map(_.toLong),
    lastCheckpointProcessedData = raw.get("lastCheckpointProcessedData").map(_.toLong),
    lastCheckpointRestoreTimestamp = raw.get("lastCheckpointRestoreTimestamp").map(_.toLong),
    lastCheckpointSize = raw.get("lastCheckpointSize").map(_.toLong),
    numberOfCompletedCheckpoints = raw.get("numberOfCompletedCheckpoints").map(_.toLong),
    numberOfFailedCheckpoints = raw.get("numberOfFailedCheckpoints").map(_.toLong),
    numberOfInProgressCheckpoints = raw.get("numberOfInProgressCheckpoints").map(_.toLong),
    restartingTime = raw.get("restartingTime").map(_.toLong),
    runningTime = raw.get("runningTime").map(_.toLong),
    totalNumberOfCheckpoints = raw.get("totalNumberOfCheckpoints").map(_.toLong),
    uptime = raw.get("uptime").map(_.toLong)
  )

end FlinkJobMetrics
