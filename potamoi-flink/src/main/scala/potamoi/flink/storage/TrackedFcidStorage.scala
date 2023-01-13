package potamoi.flink.storage

import potamoi.flink.DataStoreErr
import potamoi.flink.model.Fcid
import zio.IO
import zio.stream.Stream

/**
 * Storage for tracked flink cluster fcid.
 */
trait TrackedFcidStorage extends TrackedFcidStorage.Modify with TrackedFcidStorage.Query

object TrackedFcidStorage {
  trait Modify:
    def put(fcid: Fcid): IO[DataStoreErr, Unit]
    def rm(fcid: Fcid): IO[DataStoreErr, Unit]

  trait Query:
    def list: Stream[DataStoreErr, Fcid]
    def exists(fcid: Fcid): IO[DataStoreErr, Boolean]
}
