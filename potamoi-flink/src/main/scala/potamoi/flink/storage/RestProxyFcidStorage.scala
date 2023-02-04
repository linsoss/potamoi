package potamoi.flink.storage

import potamoi.flink.FlinkDataStoreErr
import potamoi.flink.model.Fcid
import potamoi.flink.model.snapshot.FlinkRestSvcEndpoint
import zio.IO
import zio.cache.Cache
import zio.stream.Stream

/**
 * Storage for enabled flink reverse proxy ids.
 */
trait RestProxyFcidStorage extends RestProxyFcidStorage.Modify with RestProxyFcidStorage.Query {

}

object RestProxyFcidStorage {
  trait Modify:
    def put(fcid: Fcid): IO[FlinkDataStoreErr, Unit]
    def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]

  trait Query:
    def list: Stream[FlinkDataStoreErr, Fcid]
    def exists(fcid: Fcid): IO[FlinkDataStoreErr, Boolean]
}
