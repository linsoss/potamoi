package potamoi.kubernetes.model

import potamoi.kubernetes.model.{WorkloadCondStatus, WorkloadCondStatuses, WorkloadCondition}
import zio.json.{DeriveJsonCodec, JsonCodec, JsonDecoder, JsonEncoder}

import scala.util.Try

/**
 * Kubernetes workload conditions.
 * see: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-conditions
 */
case class WorkloadCondition(
    condType: String,
    status: WorkloadCondStatus,
    reason: Option[String],
    message: Option[String],
    lastTransitionTime: Option[Long])

object WorkloadCondition:
  import WorkloadCondStatuses.given
  given JsonCodec[WorkloadCondition] = DeriveJsonCodec.gen[WorkloadCondition]
  given Ordering[WorkloadCondition]  = Ordering.by(e => e.lastTransitionTime.getOrElse(0L))

enum WorkloadCondStatus:
  case True, False, Unknown

object WorkloadCondStatuses:
  given JsonCodec[WorkloadCondStatus] = JsonCodec(
    JsonEncoder[String].contramap(_.toString),
    JsonDecoder[String].map(s => Try(WorkloadCondStatus.valueOf(s)).getOrElse(WorkloadCondStatus.Unknown))
  )
