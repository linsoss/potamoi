package potamoi.flink.storage.mem

import potamoi.flink.{FlinkDataStoreErr, FlinkMajorVer}
import potamoi.flink.model.interact.{InteractSession, InteractSessionStatus, InterpreterPod}
import potamoi.flink.storage.InteractSessionStorage
import zio.{stream, IO, Ref, UIO, ZIO}

import scala.collection.mutable

object InteractSessionMemoryStorage:

  def make: UIO[InteractSessionStorage] =
    for {
      sessRef <- Ref.make[mutable.Map[String, InteractSession]](mutable.Map.empty)
      podRef  <- Ref.make[mutable.Map[(FlinkMajorVer, String, Int), InterpreterPod]](mutable.Map.empty)
    } yield new InteractSessionStorage {
      lazy val session = SessionMemoryStorage(sessRef)
      lazy val pod     = PodMemoryStorage(podRef)
    }

class SessionMemoryStorage(ref: Ref[mutable.Map[String, InteractSession]]) extends InteractSessionStorage.SessionStorage:
  private val stg                                                            = MapBasedStg(ref)
  def put(session: InteractSession): IO[FlinkDataStoreErr, Unit]             = stg.put(session.sessionId, session)
  def rm(session: InteractSession): IO[FlinkDataStoreErr, Unit]              = stg.delete(session.sessionId)
  def get(sessionId: String): IO[FlinkDataStoreErr, Option[InteractSession]] = stg.get(sessionId)
  def list: stream.Stream[FlinkDataStoreErr, InteractSession]                = stg.streamValues
  def listSessionId: IO[FlinkDataStoreErr, List[String]]                     = stg.getKeys

class PodMemoryStorage(ref: Ref[mutable.Map[(FlinkMajorVer, String, Int), InterpreterPod]]) extends InteractSessionStorage.PodStorage:
  private val stg                                                                          = MapBasedStg(ref)
  def put(pod: InterpreterPod): IO[FlinkDataStoreErr, Unit]                                = stg.put((pod.flinkVer, pod.host, pod.port), pod)
  def rm(flinkVer: FlinkMajorVer, address: String, port: Int): IO[FlinkDataStoreErr, Unit] = stg.delete((flinkVer, address, port))
  def list: IO[FlinkDataStoreErr, List[InterpreterPod]]                                    = stg.getValues
  def listFlinkVersion: IO[FlinkDataStoreErr, Set[FlinkMajorVer]]                          = stg.getKeys.map(_.map(_._1).toSet)
  def exists(flinkVer: FlinkMajorVer): IO[FlinkDataStoreErr, Boolean]                      = stg.getKeys.map(_.exists(_._1 == flinkVer))
