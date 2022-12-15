package potamoi.flink.observer

/**
 * Flink cluster on kubernetes observer.
 */
trait FlinkObserver {
  def manager: TrackManager
  def clusters: ClustersQuery
  def jobs: JobsQuery
  def restEndpoints: RestEndpointsQuery
  def savepointTriggers: SavepointTriggersQuery
  def k8sRefs: K8sRefQuery
}
