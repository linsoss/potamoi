package potamoi.flink.storage.mem

import potamoi.flink.model.interact.{InteractSession, InteractSessionStatus}
import potamoi.flink.storage.InteractSessionStorage
import potamoi.flink.FlinkDataStoreErr
import zio.{stream, IO, Ref, UIO, ZIO}

import scala.collection.mutable

object InteractSessionMemoryStorage:
  def make: UIO[InteractSessionStorage] =
    for {
      ref <- Ref.make[mutable.Map[String, InteractSession]](mutable.Map.empty)
    } yield InteractSessionMemoryStorage(ref)

class InteractSessionMemoryStorage(ref: Ref[mutable.Map[String, InteractSession]]) extends InteractSessionStorage:
  private val stg                                                            = MapBasedStg(ref)
  def put(session: InteractSession): IO[FlinkDataStoreErr, Unit]             = stg.put(session.sessionId, session)
  def rm(session: InteractSession): IO[FlinkDataStoreErr, Unit]              = stg.delete(session.sessionId)
  def get(sessionId: String): IO[FlinkDataStoreErr, Option[InteractSession]] = stg.get(sessionId)
  def list: stream.Stream[FlinkDataStoreErr, InteractSession]                = stg.streamValues
  def listSessionId: IO[FlinkDataStoreErr, List[String]]                     = stg.getKeys
