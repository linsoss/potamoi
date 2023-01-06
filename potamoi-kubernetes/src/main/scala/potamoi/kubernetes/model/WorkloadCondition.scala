package potamoi.kubernetes.model

import potamoi.kubernetes.model.{WorkloadCondition, WorkloadCondStatus, WorkloadCondStatuses}
import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}
import potamoi.codecs
import potamoi.kubernetes.model.WorkloadCondStatuses.given_JsonCodec_WorkloadCondStatus

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
    derives JsonCodec

object WorkloadCondition:
  given Ordering[WorkloadCondition] = Ordering.by(e => e.lastTransitionTime.getOrElse(0L))

enum WorkloadCondStatus:
  case True, False, Unknown

object WorkloadCondStatuses:
  given JsonCodec[WorkloadCondStatus] = codecs.simpleEnumJsonCodec(WorkloadCondStatus.values)
