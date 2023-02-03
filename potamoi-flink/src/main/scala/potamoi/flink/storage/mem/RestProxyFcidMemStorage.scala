package potamoi.flink.storage.mem

import akka.actor.typed.Behavior
import potamoi.akka.{AkkaMatrix, DDataConf, ORSetDData}
import potamoi.flink.model.Fcid
import potamoi.flink.storage.RestProxyFcidStorage
import potamoi.flink.FlinkDataStoreErr
import zio.ZIO
import zio.stream.ZStream

import scala.collection.mutable

/**
 * Flink rest fcid list storage in-memory DData storage.
 */
object RestProxyFcidMemStorage:

  private object DData extends ORSetDData[Fcid]("rest-proxy-fcid-stg") {
    def apply(): Behavior[Req] = behavior(DDataConf.default)
  }

  def make: ZIO[AkkaMatrix, Throwable, RestProxyFcidStorage] = for {
    matrix           <- ZIO.service[AkkaMatrix]
    stg              <- matrix.spawn("rest-proxy-fcid-store", DData())
    given AkkaMatrix = matrix

  } yield new RestProxyFcidStorage {
    def put(fcid: Fcid): DIO[Unit]       = stg.put(fcid).uop
    def rm(fcid: Fcid): DIO[Unit]        = stg.remove(fcid).uop
    def list: DStream[Fcid]              = ZStream.fromIterableZIO(stg.list().rop)
    def exists(fcid: Fcid): DIO[Boolean] = stg.contains(fcid).rop
  }
