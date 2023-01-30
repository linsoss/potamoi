package potamoi.flink.storage

import potamoi.flink.{FlinkDataStoreErr, FlinkMajorVer}
import potamoi.flink.model.interact.InteractSession
import zio.IO
import zio.stream.Stream

/**
 * Flink interactive session storage.
 */
trait InteractSessionStorage extends InteractSessionStorage.Query with InteractSessionStorage.Modify

object InteractSessionStorage:
  trait Modify:
    def put(session: InteractSession): IO[FlinkDataStoreErr, Unit]
    def rm(sessionId: String): IO[FlinkDataStoreErr, Unit]

  trait Query:
    def get(sessionId: String): IO[FlinkDataStoreErr, Option[InteractSession]]
    def list: Stream[FlinkDataStoreErr, InteractSession]
    def listSessionId: IO[FlinkDataStoreErr, List[String]]
