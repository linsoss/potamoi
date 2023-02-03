package potamoi.flink.storage.mem

import akka.actor.typed.Behavior
import potamoi.akka.{AkkaMatrix, DDataConf, LWWMapDData}
import potamoi.flink.{FlinkDataStoreErr, FlinkMajorVer}
import potamoi.flink.model.interact.InteractSession
import potamoi.flink.storage.{InteractSessionStorage, K8sRefSnapStorage}
import zio.{stream, IO, ZIO}
import zio.stream.ZStream

import scala.collection.mutable

object InteractSessionMemStorage:

  private object DData extends LWWMapDData[String, InteractSession]("flink-interact-session-stg"):
    def apply(): Behavior[Req] = behavior(DDataConf.default)

  def make: ZIO[AkkaMatrix, Throwable, InteractSessionStorage] = for {
    matrix           <- ZIO.service[AkkaMatrix]
    stg              <- matrix.spawn("flink-interact-session-store", DData())
    given AkkaMatrix = matrix

  } yield new InteractSessionStorage {
    def put(session: InteractSession): DIO[Unit]             = stg.put(session.sessionId, session).uop
    def rm(sessionId: String): DIO[Unit]                     = stg.remove(sessionId).uop
    def get(sessionId: String): DIO[Option[InteractSession]] = stg.get(sessionId).rop
    def list: DStream[InteractSession]                       = ZStream.fromIterableZIO(stg.listValues().rop)
    def listSessionId: DIO[List[String]]                     = stg.listKeys().rop
  }
