package potamoi.flink.storage

import potamoi.flink.model.{Fcid, Fjid, FlinkClusterOverview, FlinkJobOverview, FlinkTmDetail, Ftid}
import potamoi.flink.DataStorageErr
import zio.IO
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
