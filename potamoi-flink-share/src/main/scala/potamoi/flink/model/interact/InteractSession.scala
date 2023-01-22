package potamoi.flink.model.interact

import potamoi.flink.{FlinkMajorVer, FlinkVersion}

case class InteractSession(
    sessionId: String,
    flinkVer: FlinkMajorVer,
    definition: InteractSessionDef,
    status: InteractSessionStatus,
    createdAt: Long)


enum InteractSessionStatus:
  case Busy
  case Idle
  case Close