package potamoi.flink.storage.mem

import potamoi.flink.storage.{RestEndpointStorage, TrackedFcidStorage}
import potamoi.flink.model.{Fcid, FlinkRestSvcEndpoint}
import potamoi.flink.DataStorageErr
import zio.{IO, Ref, UIO}
import zio.stream.{Stream, ZStream}

import scala.collection.mutable

/**
 * Tracked flink cluster fcid in-memory implementation.
 */
object TrackedFcidMemoryStorage:
  def instance: UIO[TrackedFcidStorage] = Ref.make(mutable.Set.empty[Fcid]).map(TrackedFcidMemoryStorage(_))

class TrackedFcidMemoryStorage(ref: Ref[mutable.Set[Fcid]]) extends TrackedFcidStorage:
  def put(fcid: Fcid): IO[DataStorageErr, Unit]       = ref.update(_ += fcid)
  def rm(fcid: Fcid): IO[DataStorageErr, Unit]        = ref.update(_ -= fcid)
  def list: Stream[DataStorageErr, Fcid]              = ZStream.fromIterableZIO(ref.get)
  def exists(fcid: Fcid): IO[DataStorageErr, Boolean] = ref.get.map(_.contains(fcid))