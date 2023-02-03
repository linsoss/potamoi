package potamoi.flink.storage.mem

import akka.actor.typed.Behavior
import potamoi.akka.{AkkaMatrix, DDataConf, LWWMapDData}
import potamoi.flink.FlinkDataStoreErr
import potamoi.flink.model.{Fcid, FlinkRestSvcEndpoint}
import potamoi.flink.storage.RestEndpointStorage
import zio.ZIO
import zio.stream.ZStream

import scala.collection.mutable

/**
 * Flink rest endpoint storage in-memory DData implementation.
 */
object RestEndpointMemStorage:

  private object DData extends LWWMapDData[Fcid, FlinkRestSvcEndpoint]("rest-endpoint-stg") {
    def apply(): Behavior[Req] = behavior(DDataConf.default)
  }

  def make: ZIO[AkkaMatrix, Throwable, RestEndpointStorage] = for {
    matrix           <- ZIO.service[AkkaMatrix]
    stg              <- matrix.spawn("rest-endpoint-store", DData())
    given AkkaMatrix = matrix

  } yield new RestEndpointStorage {
    def put(fcid: Fcid, endpoint: FlinkRestSvcEndpoint): DIO[Unit] = stg.put(fcid, endpoint).uop
    def rm(fcid: Fcid): DIO[Unit]                                  = stg.remove(fcid).uop
    def get(fcid: Fcid): DIO[Option[FlinkRestSvcEndpoint]]         = stg.get(fcid).rop
    def list: DStream[FlinkRestSvcEndpoint]                        = ZStream.fromIterableZIO(stg.listValues().rop)
  }
