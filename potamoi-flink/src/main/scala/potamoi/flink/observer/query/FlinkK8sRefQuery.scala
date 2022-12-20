package potamoi.flink.observer.query

import com.coralogix.zio.k8s.client.model.K8sNamespace
import com.coralogix.zio.k8s.model.apps.v1.DeploymentSpec
import com.coralogix.zio.k8s.client.model.{label, K8sNamespace}
import com.coralogix.zio.k8s.model.core.v1.{PodSpec, ServiceSpec}
import potamoi.common.Err
import potamoi.flink.{DataStorageErr, FlinkErr}
import potamoi.flink.model.{Fcid, FlinkK8sDeploymentSnap, FlinkK8sPodSnap, FlinkK8sRef, FlinkK8sRefSnap, FlinkK8sServiceSnap}
import potamoi.flink.storage.*
import potamoi.kubernetes.K8sErr.{ConfigMapNotFound, DeploymentNotFound, PodNotFound, RequestK8sApiErr, ServiceNotFound}
import potamoi.kubernetes.K8sOperator
import potamoi.syntax.valueToSome
import zio.{IO, ZIO, ZIOAppDefault}
import zio.stream.{Stream, ZStream}

/**
 * Flink k8s resource observer.
 */
trait FlinkK8sRefQuery {

  def deployment: FlinkK8sDeploymentQuery
  def service: FlinkK8sServiceQuery
  def pod: FlinkK8sPodQuery
  def podMetrics: K8sPodMetricsStorage.Query
  def configmap: FlinkConfigmapQuery

  /**
   * Get k8s resource ref of the specified fcid.
   */
  def getRef(fcid: Fcid): IO[DataStorageErr, FlinkK8sRef]

  /**
   * Get k8s resource snapshot info of the specified fcid.
   */
  def getRefSnap(fcid: Fcid): IO[DataStorageErr, FlinkK8sRefSnap]

  /**
   * Scan for potential flink clusters on the specified kubernetes namespace.
   */
  def scanK8sNamespace(namespace: String): Stream[FlinkErr, Fcid]
}

trait FlinkK8sDeploymentQuery(stg: K8sDeploymentSnapStorage) extends K8sDeploymentSnapStorage.Query {
  override def get(fcid: Fcid, deployName: String): IO[DataStorageErr, Option[FlinkK8sDeploymentSnap]] = stg.get(fcid, deployName)
  override def list(fcid: Fcid): IO[DataStorageErr, List[FlinkK8sDeploymentSnap]]                      = stg.list(fcid)
  override def listName(fcid: Fcid): IO[DataStorageErr, List[String]]                                  = stg.listName(fcid)
  def getSpec(fcid: Fcid, deployName: String): IO[RequestK8sApiErr, Option[DeploymentSpec]]
}

trait FlinkK8sServiceQuery(stg: K8sServiceSnapStorage) extends K8sServiceSnapStorage.Query {
  override def get(fcid: Fcid, svcName: String): IO[DataStorageErr, Option[FlinkK8sServiceSnap]] = stg.get(fcid, svcName)
  override def list(fcid: Fcid): IO[DataStorageErr, List[FlinkK8sServiceSnap]]                   = stg.list(fcid)
  override def listName(fcid: Fcid): IO[DataStorageErr, List[String]]                            = stg.listName(fcid)
  def getSpec(fcid: Fcid, serviceName: String): IO[RequestK8sApiErr, Option[ServiceSpec]]
}

trait FlinkK8sPodQuery(stg: K8sPodSnapStorage) extends K8sPodSnapStorage.Query {
  override def get(fcid: Fcid, podName: String): IO[DataStorageErr, Option[FlinkK8sPodSnap]] = stg.get(fcid, podName)
  override def list(fcid: Fcid): IO[DataStorageErr, List[FlinkK8sPodSnap]]                   = stg.list(fcid)
  override def listName(fcid: Fcid): IO[DataStorageErr, List[String]]                        = stg.listName(fcid)
  def getSpec(fcid: Fcid, podName: String): IO[RequestK8sApiErr, Option[PodSpec]]
}

trait FlinkConfigmapQuery(stg: K8sConfigmapNamesStorage) extends K8sConfigmapNamesStorage.Query {
  override def listName(fcid: Fcid): IO[DataStorageErr, List[String]] = stg.listName(fcid)
  def getData(fcid: Fcid, configmapName: String): IO[RequestK8sApiErr, Map[String, String]]
}

/**
 * Default implementation.
 */
case class FlinkK8sRefQueryLive(storage: K8sRefSnapStorage, k8sOperator: K8sOperator) extends FlinkK8sRefQuery {

  lazy val deployment = new FlinkK8sDeploymentQuery(storage.deployment):
    def getSpec(fcid: Fcid, deployName: String): IO[RequestK8sApiErr, Option[DeploymentSpec]] =
      k8sOperator
        .getDeploymentSpec(deployName, fcid.namespace)
        .map(e => Some(e))
        .catchSome { case DeploymentNotFound(_, _) => ZIO.succeed(None) }
        .refineToOrDie[RequestK8sApiErr]

  lazy val service = new FlinkK8sServiceQuery(storage.service):
    def getSpec(fcid: Fcid, serviceName: String): IO[RequestK8sApiErr, Option[ServiceSpec]] =
      k8sOperator
        .getServiceSpec(serviceName, fcid.namespace)
        .map(e => Some(e))
        .catchSome { case ServiceNotFound(_, _) => ZIO.succeed(None) }
        .refineToOrDie[RequestK8sApiErr]

  lazy val pod = new FlinkK8sPodQuery(storage.pod):
    def getSpec(fcid: Fcid, podName: String): IO[RequestK8sApiErr, Option[PodSpec]] =
      k8sOperator
        .getPodSpec(podName, fcid.namespace)
        .map(e => Some(e))
        .catchSome { case PodNotFound(_, _) => ZIO.succeed(None) }
        .refineToOrDie[RequestK8sApiErr]

  lazy val configmap = new FlinkConfigmapQuery(storage.configmap):
    override def getData(fcid: Fcid, configmapName: String): IO[RequestK8sApiErr, Map[String, String]] =
      k8sOperator
        .getConfigMapsData(configmapName, fcid.namespace)
        .catchSome { case ConfigMapNotFound(_, _) => ZIO.succeed(Map.empty) }
        .refineToOrDie[RequestK8sApiErr]

  lazy val podMetrics: K8sPodMetricsStorage.Query = storage.podMetrics

  override def scanK8sNamespace(namespace: String): Stream[FlinkErr, Fcid] =
    ZStream
      .fromZIO(k8sOperator.client)
      .flatMap { client =>
        client.deployments
          .getAll(K8sNamespace(namespace), labelSelector = label("type") === "flink-native-kubernetes")
          .mapBoth(
            f => FlinkErr.K8sFail(RequestK8sApiErr(f)),
            deploy => deploy.metadata.flatMap(_.name).toOption
          )
          .filter(_.isDefined)
          .map(name => Fcid(name.get, namespace))
      }

  override def getRef(fcid: Fcid): IO[DataStorageErr, FlinkK8sRef] = {
    deployment.listName(fcid) <&>
    service.listName(fcid) <&>
    pod.listName(fcid) <&>
    configmap.listName(fcid)
  } map { case (deploys, services, pods, configmaps) =>
    FlinkK8sRef(fcid.clusterId, fcid.namespace, deploys, services, pods, configmaps)
  }

  override def getRefSnap(fcid: Fcid): IO[DataStorageErr, FlinkK8sRefSnap] = {
    deployment.list(fcid) <&>
    service.list(fcid) <&>
    pod.list(fcid)
  } map { case (deploys, services, pods) =>
    FlinkK8sRefSnap(fcid.clusterId, fcid.namespace, deploys, services, pods)
  }

}