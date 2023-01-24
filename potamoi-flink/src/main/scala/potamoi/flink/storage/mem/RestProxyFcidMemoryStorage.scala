package potamoi.flink.storage.mem

import potamoi.flink.model.Fcid
import potamoi.flink.storage.RestProxyFcidStorage
import potamoi.flink.FlinkDataStoreErr
import zio.{IO, Ref, UIO}
import zio.stream.{Stream, ZStream}

import scala.collection.mutable

/**
 * Flink rest fcid list storage in-memory storage.
 */
object RestProxyFcidMemoryStorage:
  def make: UIO[RestProxyFcidStorage] = Ref.make(mutable.Set.empty[Fcid]).map(RestProxyFcidMemoryStorage(_))

class RestProxyFcidMemoryStorage(ref: Ref[mutable.Set[Fcid]]) extends RestProxyFcidStorage:
  def put(fcid: Fcid): IO[FlinkDataStoreErr, Unit]       = ref.update(_ += fcid)
  def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]        = ref.update(_ -= fcid)
  def list: Stream[FlinkDataStoreErr, Fcid]              = ZStream.fromIterableZIO(ref.get)
  def exists(fcid: Fcid): IO[FlinkDataStoreErr, Boolean] = ref.get.map(_.contains(fcid))
