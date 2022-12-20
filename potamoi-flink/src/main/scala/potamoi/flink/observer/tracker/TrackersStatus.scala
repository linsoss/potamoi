package potamoi.flink.observer.tracker

import potamoi.flink.model.Fcid
import zio.json.{DeriveJsonCodec, JsonCodec, JsonDecoder, JsonEncoder}

import scala.util.Try

/**
 * Flink snapshot trackers status.
 */
case class TrackersStatus(fcid: Fcid, clusterTracker: TrackerState, k8sRefTracker: TrackerState)

object TrackersStatus:
  import TrackerStates.given
  given JsonCodec[TrackersStatus] = DeriveJsonCodec.gen[TrackersStatus]

enum TrackerState:
  case Running, Idle, Unknown

object TrackerStates:
  given JsonCodec[TrackerState] = JsonCodec(
    JsonEncoder[String].contramap(_.toString),
    JsonDecoder[String].map(s => Try(TrackerState.valueOf(s)).getOrElse(TrackerState.Unknown))
  )
