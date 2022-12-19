package potamoi.flink.observer.query

import com.coralogix.zio.k8s.model.apps.v1.DeploymentSpec
import com.coralogix.zio.k8s.model.core.v1.{PodSpec, ServiceSpec}
import potamoi.flink.{DataStorageErr, FlinkErr}
import potamoi.flink.model.{Fcid, FlinkK8sRef, FlinkK8sRefSnap}
import potamoi.flink.storage.*
import potamoi.kubernetes.K8sErr.RequestK8sApiErr
import zio.IO
import zio.stream.Stream

/**
 * Flink k8s resource observer.
 */
trait FlinkK8sRefQuery {
  def deployment: FlinkK8sDeploymentQuery
  def service: FlinkK8sServiceQuery
  def pod: FlinkK8sPodQuery
  def podMetrics: K8sPodMetricsStorage.Query
  def configmap: FlinkConfigmapQuery

  def getRef(fcid: Fcid): IO[DataStorageErr, FlinkK8sRef] = {
    deployment.listName(fcid) <&>
    service.listName(fcid) <&>
    pod.listName(fcid) <&>
    configmap.listName(fcid)
  } map { case (deploys, services, pods, configmaps) =>
    FlinkK8sRef(fcid.clusterId, fcid.namespace, deploys, services, pods, configmaps)
  }

  def getRefSnap(fcid: Fcid): IO[DataStorageErr, FlinkK8sRefSnap] = {
    deployment.list(fcid) <&>
    service.list(fcid) <&>
    pod.list(fcid)
  } map { case (deploys, services, pods) =>
    FlinkK8sRefSnap(fcid.clusterId, fcid.namespace, deploys, services, pods)
  }

  /**
   * Scan for potential flink clusters on the specified kubernetes namespace.
   */
  def scanK8sNamespace(namespace: String): Stream[FlinkErr, Fcid]
}

trait FlinkK8sDeploymentQuery extends K8sDeploymentSnapStorage.Query:
  def getSpec(fcid: Fcid, deployName: String): IO[RequestK8sApiErr, Option[DeploymentSpec]]

trait FlinkK8sServiceQuery extends K8sServiceSnapStorage.Query:
  def getSpec(fcid: Fcid, serviceName: String): IO[RequestK8sApiErr, Option[ServiceSpec]]

trait FlinkK8sPodQuery extends K8sPodSnapStorage.Query:
  def getSpec(fcid: Fcid, podName: String): IO[RequestK8sApiErr, Option[PodSpec]]

trait FlinkConfigmapQuery extends K8sConfigmapNamesStorage.Query:
  def getData(fcid: Fcid, configmapName: String): IO[FlinkErr, Map[String, String]]
