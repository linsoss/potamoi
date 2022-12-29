package potamoi.flink.storage

import potamoi.flink.DataStorageErr
import potamoi.flink.model.{Fcid, FlinkRestSvcEndpoint}
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
    def put(fcid: Fcid): IO[DataStorageErr, Unit]
    def rm(fcid: Fcid): IO[DataStorageErr, Unit]

  trait Query:
    def list: Stream[DataStorageErr, Fcid]
    def exists(fcid: Fcid): IO[DataStorageErr, Boolean]
}
