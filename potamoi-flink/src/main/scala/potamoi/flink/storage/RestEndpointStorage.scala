package potamoi.flink.storage

import potamoi.flink.DataStorageErr
import potamoi.flink.model.{Fcid, FlinkRestSvcEndpoint}
import zio.IO
import zio.stream.Stream

/**
 * Storage for flink rest endpoint.
 */
trait RestEndpointStorage extends RestEndpointStorage.Modify with RestEndpointStorage.Query

object RestEndpointStorage {
  trait Modify:
    def put(fcid: Fcid, endpoint: FlinkRestSvcEndpoint): IO[DataStorageErr, Unit]
    def rm(fcid: Fcid): IO[DataStorageErr, Unit]

  trait Query:
    def get(fcid: Fcid): IO[DataStorageErr, Option[FlinkRestSvcEndpoint]]
    def list: Stream[DataStorageErr, FlinkRestSvcEndpoint]
}
