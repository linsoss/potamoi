package potamoi.flink.storage.mem

import potamoi.flink.DataStorageErr
import potamoi.flink.model.{Fcid, FlinkRestSvcEndpoint}
import potamoi.flink.storage.RestEndpointStorage
import zio.{IO, Ref}
import zio.stream.Stream

import scala.collection.mutable

/**
 * Flink rest endpoint storage in-memory implementation.
 */
object RestEndpointMemoryStorage:
  def instance = Ref.make(mutable.Map.empty[Fcid, FlinkRestSvcEndpoint]).map(RestEndpointMemoryStorage(_))

class RestEndpointMemoryStorage(ref: Ref[mutable.Map[Fcid, FlinkRestSvcEndpoint]]) extends RestEndpointStorage:
  private val stg                                                               = MapBasedStg(ref)
  def put(fcid: Fcid, endpoint: FlinkRestSvcEndpoint): IO[DataStorageErr, Unit] = stg.put(fcid, endpoint)
  def rm(fcid: Fcid): IO[DataStorageErr, Unit]                                  = stg.delete(fcid)
  def get(fcid: Fcid): IO[DataStorageErr, Option[FlinkRestSvcEndpoint]]         = stg.get(fcid)
  def list: Stream[DataStorageErr, FlinkRestSvcEndpoint]                        = stg.streamValues
