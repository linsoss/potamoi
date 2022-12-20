package potamoi.flink.storage

import potamoi.flink.model.Fcid
import potamoi.flink.DataStorageErr
import zio.IO
import zio.stream.Stream

/**
 * Storage for tracked flink cluster fcid.
 */
trait TrackedFcidStorage extends TrackedFcidStorage.Modify with TrackedFcidStorage.Query

object TrackedFcidStorage {
  trait Modify:
    def put(fcid: Fcid): IO[DataStorageErr, Unit]
    def rm(fcid: Fcid): IO[DataStorageErr, Unit]

  trait Query:
    def list: Stream[DataStorageErr, Fcid]
    def exists(fcid: Fcid): IO[DataStorageErr, Boolean]
}
  