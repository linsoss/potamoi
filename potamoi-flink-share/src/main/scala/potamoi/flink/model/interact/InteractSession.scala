package potamoi.flink.model.interact

import potamoi.curTs
import potamoi.flink.{FlinkMajorVer, FlinkVersion}

case class InteractSession(
    sessionId: String,
    flinkVer: FlinkMajorVer,
//    status: InteractSessionStatus,
    createdAt: Long = curTs)

enum InteractSessionStatus:
  case Busy
  case Idle
  case Close
