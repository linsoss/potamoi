package potamoi.flink.storage

import potamoi.flink.FlinkDataStoreErr
import potamoi.flink.model.Fcid
import potamoi.flink.model.snapshot.FlinkRestSvcEndpoint
import zio.IO
import zio.stream.Stream

/**
 * Storage for flink rest endpoint.
 */
trait RestEndpointStorage extends RestEndpointStorage.Modify with RestEndpointStorage.Query

object RestEndpointStorage {
  trait Modify:
    def put(fcid: Fcid, endpoint: FlinkRestSvcEndpoint): IO[FlinkDataStoreErr, Unit]
    def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]

  trait Query:
    def get(fcid: Fcid): IO[FlinkDataStoreErr, Option[FlinkRestSvcEndpoint]]
    def list: Stream[FlinkDataStoreErr, FlinkRestSvcEndpoint]
}
