package potamoi.flink.observer

/**
 * Flink cluster on kubernetes observer.
 */
trait FlinkObserver {
  def manager: TrackManager
  def clusters: ClustersQuery
  def jobs: JobsQuery
  def restEndpoints: RestEndpointQuery
  def savepointTriggers: SavepointTriggerQuery
  def k8sRefs: K8sRefQuery
}
