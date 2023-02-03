package potamoi.flink.storage.mem

import akka.actor.typed.Behavior
import potamoi.akka.{AkkaMatrix, DDataConf, ORSetDData}
import potamoi.flink.model.{Fcid, FlinkRestSvcEndpoint}
import potamoi.flink.storage.{RestEndpointStorage, TrackedFcidStorage}
import potamoi.flink.FlinkDataStoreErr
import zio.ZIO
import zio.stream.ZStream

/**
 * Tracked flink cluster fcid in-memory DData implementation.
 */
object TrackedFcidMemStorage:

  private object DData extends ORSetDData[Fcid]("tracked-fcid-stg") {
    def apply(): Behavior[Req] = behavior(DDataConf.default)
  }

  def make: ZIO[AkkaMatrix, Throwable, TrackedFcidStorage] = for {
    matrix           <- ZIO.service[AkkaMatrix]
    stg              <- matrix.spawn("tracked-fcid-store", DData())
    given AkkaMatrix = matrix

  } yield new TrackedFcidStorage {
    def put(fcid: Fcid): DIO[Unit]       = stg.put(fcid).uop
    def rm(fcid: Fcid): DIO[Unit]        = stg.remove(fcid).uop
    def list: DStream[Fcid]              = ZStream.fromIterableZIO(stg.list().rop)
    def exists(fcid: Fcid): DIO[Boolean] = stg.contains(fcid).rop
  }
