package potamoi.flink.observer

import com.coralogix.zio.k8s.model.apps.v1.DeploymentSpec
import com.coralogix.zio.k8s.model.core.v1.{PodSpec, ServiceSpec}
import potamoi.flink.model.{Fcid, Fjid, FlinkRestSvcEndpoint, FlinkSptTriggerStatus}
import potamoi.flink.FlinkErr
import potamoi.flink.observer.query.{FlinkClusterQuery, FlinkJobQuery, FlinkK8sRefQuery, FlinkRestEndpointQuery}
import potamoi.kubernetes.K8sErr.RequestK8sApiErr
import zio.IO
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
