package potamoi.flink.observer

import com.coralogix.zio.k8s.model.apps.v1.DeploymentSpec
import com.coralogix.zio.k8s.model.core.v1.{PodSpec, ServiceSpec}
import com.devsisters.shardcake.Sharding
import potamoi.flink.model.{Fcid, Fjid, FlinkRestSvcEndpoint, FlinkSptTriggerStatus}
import potamoi.flink.{FlinkConf, FlinkErr, FlinkRestEndpointRetrieverLive}
import potamoi.flink.observer.query.{
  FlinkClusterQuery,
  FlinkClusterQueryLive,
  FlinkJobQuery,
  FlinkJobQueryLive,
  FlinkK8sRefQuery,
  FlinkK8sRefQueryLive,
  FlinkRestEndpointQuery,
  FlinkRestEndpointQueryLive
}
import potamoi.flink.observer.tracker.{TrackManager, TrackerManager}
import potamoi.flink.storage.FlinkSnapshotStorage
import potamoi.kubernetes.K8sErr.RequestK8sApiErr
import potamoi.kubernetes.K8sOperator
import zio.{IO, URIO, ZIO}
import zio.stream.Stream

import scala.concurrent.duration.Duration

/**
 * Flink cluster on kubernetes observer.
 */
trait FlinkObserver {
  def manager: TrackManager
  def restEndpoint: FlinkRestEndpointQuery
  def cluster: FlinkClusterQuery
  def job: FlinkJobQuery
  def k8s: FlinkK8sRefQuery
}

object FlinkObserver {

  val live: URIO[Sharding with FlinkSnapshotStorage with K8sOperator with FlinkConf, FlinkObserver] =
    for {
      flinkConf   <- ZIO.service[FlinkConf]
      k8sOperator <- ZIO.service[K8sOperator]
      snapStorage <- ZIO.service[FlinkSnapshotStorage]
      k8sClient   <- k8sOperator.client
      eptRetriever      = FlinkRestEndpointRetrieverLive(k8sClient)
      restEndpointQuery = FlinkRestEndpointQueryLive(snapStorage.restEndpoint, eptRetriever)
      clusterQuery      = FlinkClusterQueryLive(snapStorage.cluster)
      jobQuery          = FlinkJobQueryLive(flinkConf, snapStorage.job, restEndpointQuery)
      k8sRefQuery       = FlinkK8sRefQueryLive(snapStorage.k8sRef, k8sOperator)
      trackerManager <- TrackerManager.instance(flinkConf, snapStorage, eptRetriever, k8sOperator)
    } yield new FlinkObserver:
      val manager           = trackerManager
      lazy val restEndpoint = restEndpointQuery
      lazy val cluster      = clusterQuery
      lazy val job          = jobQuery
      lazy val k8s          = k8sRefQuery
}
