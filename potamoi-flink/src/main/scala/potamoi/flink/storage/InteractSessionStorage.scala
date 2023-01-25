package potamoi.flink.storage

import potamoi.flink.model.interact.{InteractSession, InteractSessionStatus}
import potamoi.flink.FlinkDataStoreErr
import zio.IO
import zio.stream.Stream

trait InteractSessionStorage extends InteractSessionStorage.Modify with InteractSessionStorage.Query

object InteractSessionStorage:
  trait Modify:
    def put(session: InteractSession): IO[FlinkDataStoreErr, Unit]
    def rm(session: InteractSession): IO[FlinkDataStoreErr, Unit]

  trait Query:
    def get(sessionId: String): IO[FlinkDataStoreErr, Option[InteractSession]]
    def list: Stream[FlinkDataStoreErr, InteractSession]
    def listSessionId: IO[FlinkDataStoreErr, List[String]]
