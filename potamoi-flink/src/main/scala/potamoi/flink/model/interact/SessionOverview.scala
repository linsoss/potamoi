package potamoi.flink.model.interact

import zio.json.JsonCodec

/**
 * Flink interpreter session status overview.
 */
case class SessionOverview(
    sessionId: String,
    isStarted: Boolean,
    isBusy: Boolean,
    sessionDef: Option[SessionDef])
    derives JsonCodec
