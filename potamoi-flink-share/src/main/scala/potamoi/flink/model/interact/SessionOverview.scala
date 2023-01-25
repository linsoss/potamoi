package potamoi.flink.model.interact

import zio.json.JsonCodec

case class SessionOverview(
    sessionId: String,
    isStarted: Boolean,
    isBusy: Boolean,
    sessionDef: Option[SessionDef])
    derives JsonCodec
