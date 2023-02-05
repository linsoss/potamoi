package potamoi.flink.model.interact

import potamoi.KryoSerializable
import zio.json.JsonCodec

/**
 * Flink interpreter session status overview.
 */
case class SessionOverview(
    sessionId: String,
    isStarted: Boolean,
    isBusy: Boolean,
    sessionDef: Option[SessionSpec])
    extends KryoSerializable
    derives JsonCodec
