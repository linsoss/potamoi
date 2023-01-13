package potamoi.flink.storage

import potamoi.flink.DataStoreErr
import potamoi.flink.model.{Fcid, FlinkRestSvcEndpoint}
import zio.IO
import zio.stream.Stream

/**
 * Storage for flink rest endpoint.
 */
trait RestEndpointStorage extends RestEndpointStorage.Modify with RestEndpointStorage.Query

object RestEndpointStorage {
  trait Modify:
    def put(fcid: Fcid, endpoint: FlinkRestSvcEndpoint): IO[DataStoreErr, Unit]
    def rm(fcid: Fcid): IO[DataStoreErr, Unit]

  trait Query:
    def get(fcid: Fcid): IO[DataStoreErr, Option[FlinkRestSvcEndpoint]]
    def list: Stream[DataStoreErr, FlinkRestSvcEndpoint]
}
