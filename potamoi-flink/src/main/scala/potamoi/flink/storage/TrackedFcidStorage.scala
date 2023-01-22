package potamoi.flink.storage

import potamoi.flink.FlinkDataStoreErr
import potamoi.flink.model.Fcid
import zio.IO
import zio.stream.Stream

/**
 * Storage for tracked flink cluster fcid.
 */
trait TrackedFcidStorage extends TrackedFcidStorage.Modify with TrackedFcidStorage.Query

object TrackedFcidStorage {
  trait Modify:
    def put(fcid: Fcid): IO[FlinkDataStoreErr, Unit]
    def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]

  trait Query:
    def list: Stream[FlinkDataStoreErr, Fcid]
    def exists(fcid: Fcid): IO[FlinkDataStoreErr, Boolean]
}
