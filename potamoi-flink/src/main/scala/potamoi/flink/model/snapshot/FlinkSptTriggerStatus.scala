package potamoi.flink.model.snapshot

import potamoi.codecs
import potamoi.flink.model.snapshot.FlinkPipeOprStates.given
import zio.json.{DeriveJsonCodec, JsonCodec, JsonDecoder, JsonEncoder}

/**
 * Flink savepoint trigger status.
 */
case class FlinkSptTriggerStatus(state: FlinkPipeOprState, failureCause: Option[String], location: Option[String]) derives JsonCodec:
  lazy val isCompleted = state == FlinkPipeOprState.Completed
  lazy val isFailed    = failureCause.isDefined

enum FlinkPipeOprState(val rawValue: String):
  case Completed  extends FlinkPipeOprState("COMPLETED")
  case InProgress extends FlinkPipeOprState("IN_PROGRESS")
  case Unknown    extends FlinkPipeOprState("UNKNOWN")

object FlinkPipeOprStates:
  given JsonCodec[FlinkPipeOprState]             = codecs.simpleEnumJsonCodec(FlinkPipeOprState.values)
  def ofRaw(rawValue: String): FlinkPipeOprState = FlinkPipeOprState.values.find(_.rawValue == rawValue).getOrElse(FlinkPipeOprState.Unknown)
