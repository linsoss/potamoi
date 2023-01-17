package potamoi.flink.interact

import potamoi.flink.model.interact.SessionDef
import zio.IO

trait FlinkSqlInteractor:

  def createSession(sessionId: String, sessionDef: SessionDef): IO[Throwable, Unit]
  def closeSession(sessionId: String): IO[Throwable, Unit]
