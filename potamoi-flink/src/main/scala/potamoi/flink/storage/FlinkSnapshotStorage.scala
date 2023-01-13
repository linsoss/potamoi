package potamoi.flink.storage

import potamoi.flink.DataStoreErr
import potamoi.flink.model.*
import potamoi.flink.storage.mem.*
import zio.{IO, ZLayer}
import zio.stream.Stream

/**
 * Flink snapshot information storage.
 */
trait FlinkSnapshotStorage:
  self =>
  def trackedList: TrackedFcidStorage
  def restEndpoint: RestEndpointStorage
  def restProxy: RestProxyFcidStorage
  def cluster: ClusterSnapStorage
  def job: JobSnapStorage
  def k8sRef: K8sRefSnapStorage

  /**
   * Remove all current snapshot data belongs to Fcid.
   */
  def rmSnapData(fcid: Fcid): IO[DataStoreErr, Unit] = {
    restEndpoint.rm(fcid) <&>
    restProxy.rm(fcid) <&>
    cluster.rmSnapData(fcid) <&>
    job.rmSnapData(fcid) <&>
    k8sRef.rmSnapData(fcid)
  }

object FlinkSnapshotStorage:
  lazy val test = memory

  // pure in local memory implementation
  lazy val memory: ZLayer[Any, Nothing, FlinkSnapshotStorage] = ZLayer {
    for {
      trackedLists  <- TrackedFcidMemoryStorage.instance
      restEndpoints <- RestEndpointMemoryStorage.instance
      restProxies   <- RestProxyFcidMemoryStorage.instance
      clusters      <- ClusterSnapMemoryStorage.instance
      jobs          <- JobSnapMemoryStorage.instance
      k8sRefs       <- K8sRefSnapMemoryStorage.instance
    } yield new FlinkSnapshotStorage:
      lazy val trackedList  = trackedLists
      lazy val restEndpoint = restEndpoints
      lazy val restProxy    = restProxies
      lazy val cluster      = clusters
      lazy val job          = jobs
      lazy val k8sRef       = k8sRefs
  }
