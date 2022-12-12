package potamoi.kubernetes.model

import potamoi.kubernetes.model.PodPhase
import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}

import scala.util.Try

/**
 * Kubernetes pod phase.
 * see: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/
 */
enum PodPhase:
  case Pending, Running, Succeeded, Failed, Unknown

object PodPhases:
  given JsonCodec[PodPhase] = JsonCodec(
    JsonEncoder[String].contramap(_.toString),
    JsonDecoder[String].map(s => Try(PodPhase.valueOf(s)).getOrElse(PodPhase.Unknown))
  )
