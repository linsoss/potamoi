package potamoi.flink.model

import potamoi.curTs
import zio.json.{DeriveJsonCodec, JsonCodec, JsonDecoder, JsonEncoder}

/**
 * Flink job overview.
 */
case class FlinkJobOverview(
    clusterId: String,
    namespace: String,
    jobId: String,
    jobName: String,
    state: JobState,
    startTs: Long,
    endTs: Long,
    tasks: TaskStats,
    ts: Long):

  lazy val fjid: Fjid  = Fjid(clusterId, namespace, jobId)
  def durationTs: Long = curTs - startTs

case class TaskStats(
    total: Int,
    created: Int,
    scheduled: Int,
    deploying: Int,
    running: Int,
    finished: Int,
    canceling: Int,
    canceled: Int,
    failed: Int,
    reconciling: Int,
    initializing: Int)

object FlinkJobOverview:
  import JobStates.given
  given JsonCodec[TaskStats]        = DeriveJsonCodec.gen[TaskStats]
  given JsonCodec[FlinkJobOverview] = DeriveJsonCodec.gen[FlinkJobOverview]
  given Ordering[FlinkJobOverview]  = Ordering.by(e => (e.clusterId, e.namespace, e.jobId))

/**
 * Flink job state.
 * see: [[org.apache.flink.api.common.JobStatus]]
 */
enum JobState:
  case INITIALIZING, CREATED, RUNNING, FAILING, FAILED, CANCELLING, CANCELED, FINISHED, RESTARTING, SUSPENDED,
    RECONCILING, UNKNOWN

object JobStates:
  import JobState.*
  given JsonCodec[JobState] = JsonCodec(
    JsonEncoder[String].contramap(_.toString),
    JsonDecoder[String].map(s => JobState.values.find(_.toString == s).getOrElse(JobState.UNKNOWN))
  )
  lazy val InactiveStates         = Set(FAILED, CANCELED, FINISHED)
  def isActive(state: JobState)   = !InactiveStates.contains(state)
