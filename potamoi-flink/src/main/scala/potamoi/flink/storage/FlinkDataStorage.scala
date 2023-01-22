package potamoi.flink.storage

import potamoi.flink.FlinkDataStoreErr
import potamoi.flink.model.*
import potamoi.flink.storage.mem.*
import zio.{IO, ZLayer}
import zio.stream.Stream

/**
 * Flink data storage.
 */
trait FlinkDataStorage:

  def trackedList: TrackedFcidStorage
  def restEndpoint: RestEndpointStorage
  def restProxy: RestProxyFcidStorage
  def cluster: ClusterSnapStorage
  def job: JobSnapStorage
  def k8sRef: K8sRefSnapStorage

  /**
   * Remove all current snapshot data belongs to Fcid.
   */
  def rmSnapData(fcid: Fcid): IO[FlinkDataStoreErr, Unit] = {
    restEndpoint.rm(fcid) <&>
    restProxy.rm(fcid) <&>
    cluster.rmSnapData(fcid) <&>
    job.rmSnapData(fcid) <&>
    k8sRef.rmSnapData(fcid)
  }

object FlinkDataStorage:

  lazy val test = memory

  // pure in local memory implementation
  lazy val memory: ZLayer[Any, Nothing, FlinkDataStorage] = ZLayer {
    for {
      trackedLists  <- TrackedFcidMemoryStorage.instance
      restEndpoints <- RestEndpointMemoryStorage.instance
      restProxies   <- RestProxyFcidMemoryStorage.instance
      clusters      <- ClusterSnapMemoryStorage.instance
      jobs          <- JobSnapMemoryStorage.instance
      k8sRefs       <- K8sRefSnapMemoryStorage.instance
    } yield new FlinkDataStorage:
      lazy val trackedList  = trackedLists
      lazy val restEndpoint = restEndpoints
      lazy val restProxy    = restProxies
      lazy val cluster      = clusters
      lazy val job          = jobs
      lazy val k8sRef       = k8sRefs
  }
