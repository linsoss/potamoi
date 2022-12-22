package potamoi.flink.model

import zio.json.{DeriveJsonCodec, JsonCodec, JsonDecoder, JsonEncoder}

/**
 * Flink savepoint trigger status.
 */
case class FlinkSptTriggerStatus(state: FlinkPipeOprState, failureCause: Option[String]) {
  lazy val isCompleted = state == FlinkPipeOprState.Completed
  lazy val isFailed    = failureCause.isDefined
}

object FlinkSptTriggerStatus:
  import FlinkPipeOprStates.given
  given JsonCodec[FlinkSptTriggerStatus] = DeriveJsonCodec.gen[FlinkSptTriggerStatus]

enum FlinkPipeOprState(val value: String):
  case Completed  extends FlinkPipeOprState("COMPLETED")
  case InProgress extends FlinkPipeOprState("IN_PROGRESS")
  case Unknown    extends FlinkPipeOprState("UNKNOWN")

object FlinkPipeOprStates:
  given JsonCodec[FlinkPipeOprState] = JsonCodec(
    JsonEncoder[String].contramap(_.value),
    JsonDecoder[String].map(s => FlinkPipeOprState.values.find(_.value == s).getOrElse(FlinkPipeOprState.Unknown))
  )
  def ofRaw(rawValue: String): FlinkPipeOprState =
    FlinkPipeOprState.values.find(_.value == rawValue).getOrElse(FlinkPipeOprState.Unknown)
