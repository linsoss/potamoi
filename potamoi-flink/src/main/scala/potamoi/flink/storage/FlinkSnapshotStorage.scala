package potamoi.flink.storage

import potamoi.flink.model.{Fcid, Fjid, FlinkClusterOverview, FlinkJobOverview, FlinkTmDetail, Ftid}
import potamoi.flink.DataStorageErr
import potamoi.flink.storage.mem.{
  ClusterSnapMemoryStorage,
  JobSnapMemoryStorage,
  K8sRefSnapMemoryStorage,
  RestEndpointMemoryStorage,
  TrackedFcidMemoryStorage
}
import zio.{IO, ZLayer}
import zio.stream.Stream

/**
 * Flink snapshot information storage.
 */
trait FlinkSnapshotStorage:
  def trackedList: TrackedFcidStorage
  def restEndpoint: RestEndpointStorage
  def cluster: ClusterSnapStorage
  def job: JobSnapStorage
  def k8sRef: K8sRefSnapStorage

object FlinkSnapshotStorage:
  lazy val test = memory

  // pure in local memory implementation
  lazy val memory: ZLayer[Any, Nothing, FlinkSnapshotStorage] = ZLayer {
    for {
      trackedLists  <- TrackedFcidMemoryStorage.instance
      restEndpoints <- RestEndpointMemoryStorage.instance
      clusters      <- ClusterSnapMemoryStorage.instance
      jobs          <- JobSnapMemoryStorage.instance
      k8sRefs       <- K8sRefSnapMemoryStorage.instance
    } yield new FlinkSnapshotStorage {
      val trackedList  = trackedLists
      val restEndpoint = restEndpoints
      val cluster      = clusters
      val job          = jobs
      val k8sRef       = k8sRefs
    }
  }
