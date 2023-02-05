package potamoi.flink.observer.query

import com.coralogix.zio.k8s.client.model.{label, K8sNamespace}
import com.coralogix.zio.k8s.model.apps.v1.DeploymentSpec
import com.coralogix.zio.k8s.model.core.v1.{PodSpec, ServiceSpec}
import potamoi.flink.{FlinkDataStoreErr, FlinkErr}
import potamoi.flink.model.{snapshot, *}
import potamoi.flink.model.snapshot.{FlinkK8sDeploymentSnap, FlinkK8sPodSnap, FlinkK8sRef, FlinkK8sRefSnap, FlinkK8sServiceSnap}
import potamoi.flink.storage.*
import potamoi.kubernetes.K8sErr.*
import potamoi.kubernetes.K8sOperator
import potamoi.syntax.valueToSome
import zio.{IO, UIO, ZIO, ZIOAppDefault}
import zio.stream.{Stream, ZStream}

/**
 * Flink k8s resource observer.
 */
trait FlinkK8sRefQuery {

  /**
   * Query flink k8s deployment snapshots.
   */
  def deployment: FlinkK8sDeploymentQuery

  /**
   * Query flink k8s service snapshots.
   */
  def service: FlinkK8sServiceQuery

  /**
   * Query flink k8s pods snapshots.
   */
  def pod: FlinkK8sPodQuery

  /**
   * Query flink k8s metrics snapshots.
   */
  def podMetrics: K8sPodMetricsStorage.Query

  /**
   * Query flink k8s configmap snapshots.
   */
  def configmap: FlinkConfigmapQuery

  /**
   * Get k8s resource ref of the specified fcid.
   */
  def getRef(fcid: Fcid): IO[FlinkDataStoreErr, FlinkK8sRef]

  /**
   * Get k8s resource snapshot info of the specified fcid.
   */
  def getRefSnap(fcid: Fcid): IO[FlinkDataStoreErr, FlinkK8sRefSnap]

  /**
   * Scan for potential flink clusters on the specified kubernetes namespace.
   */
  def scanK8sNamespace(namespace: String): Stream[FlinkErr.K8sFailure, Fcid]

  /**
   * Only for getting flink-main-container logs, side-car container logs should be obtained
   * through the [[K8sOperator.getPodLog()]].
   */
  def viewLog(
      fcid: Fcid,
      podName: String,
      follow: Boolean = false,
      tailLines: Option[Int] = None,
      sinceSec: Option[Int] = None): Stream[FlinkErr.K8sFailure, String]
}

trait FlinkK8sDeploymentQuery(stg: K8sDeploymentSnapStorage) extends K8sDeploymentSnapStorage.Query {
  override def get(fcid: Fcid, deployName: String): IO[FlinkDataStoreErr, Option[FlinkK8sDeploymentSnap]] = stg.get(fcid, deployName)
  override def list(fcid: Fcid): IO[FlinkDataStoreErr, List[FlinkK8sDeploymentSnap]]                      = stg.list(fcid)
  override def listName(fcid: Fcid): IO[FlinkDataStoreErr, List[String]]                                  = stg.listName(fcid)
  def getSpec(fcid: Fcid, deployName: String): IO[RequestK8sApiErr, Option[DeploymentSpec]]
}

trait FlinkK8sServiceQuery(stg: K8sServiceSnapStorage) extends K8sServiceSnapStorage.Query {
  override def get(fcid: Fcid, svcName: String): IO[FlinkDataStoreErr, Option[FlinkK8sServiceSnap]] = stg.get(fcid, svcName)
  override def list(fcid: Fcid): IO[FlinkDataStoreErr, List[FlinkK8sServiceSnap]]                   = stg.list(fcid)
  override def listName(fcid: Fcid): IO[FlinkDataStoreErr, List[String]]                            = stg.listName(fcid)
  def getSpec(fcid: Fcid, serviceName: String): IO[RequestK8sApiErr, Option[ServiceSpec]]
}

trait FlinkK8sPodQuery(stg: K8sPodSnapStorage) extends K8sPodSnapStorage.Query {
  override def get(fcid: Fcid, podName: String): IO[FlinkDataStoreErr, Option[FlinkK8sPodSnap]] = stg.get(fcid, podName)
  override def list(fcid: Fcid): IO[FlinkDataStoreErr, List[FlinkK8sPodSnap]]                   = stg.list(fcid)
  override def listName(fcid: Fcid): IO[FlinkDataStoreErr, List[String]]                        = stg.listName(fcid)
  def getSpec(fcid: Fcid, podName: String): IO[RequestK8sApiErr, Option[PodSpec]]
}

trait FlinkConfigmapQuery(stg: K8sConfigmapNamesStorage) extends K8sConfigmapNamesStorage.Query {
  override def listName(fcid: Fcid): IO[FlinkDataStoreErr, List[String]] = stg.listName(fcid)
  def getData(fcid: Fcid, configmapName: String): IO[RequestK8sApiErr, Map[String, String]]
}

object FlinkK8sRefQuery {
  def make(storage: K8sRefSnapStorage, k8sOperator: K8sOperator): UIO[FlinkK8sRefQuery] =
    ZIO.succeed(FlinkK8sRefQueryImpl(storage, k8sOperator))
}

/**
 * Default implementation.
 */
class FlinkK8sRefQueryImpl(storage: K8sRefSnapStorage, k8sOperator: K8sOperator) extends FlinkK8sRefQuery {

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

  override def scanK8sNamespace(namespace: String): Stream[FlinkErr.K8sFailure, Fcid] =
    ZStream
      .from(k8sOperator.client)
      .flatMap { client =>
        client.deployments
          .getAll(K8sNamespace(namespace), labelSelector = label("type") === "flink-native-kubernetes")
          .mapBoth(
            f => FlinkErr.K8sFailure(RequestK8sApiErr(f)),
            deploy => deploy.metadata.flatMap(_.name).toOption
          )
          .filter(_.isDefined)
          .map(name => Fcid(name.get, namespace))
      }

  override def getRef(fcid: Fcid): IO[FlinkDataStoreErr, FlinkK8sRef] = {
    deployment.listName(fcid) <&>
    service.listName(fcid) <&>
    pod.listName(fcid) <&>
    configmap.listName(fcid)
  } map { case (deploys, services, pods, configmaps) =>
    FlinkK8sRef(fcid.clusterId, fcid.namespace, deploys, services, pods, configmaps)
  }

  override def getRefSnap(fcid: Fcid): IO[FlinkDataStoreErr, FlinkK8sRefSnap] = {
    deployment.list(fcid) <&>
    service.list(fcid) <&>
    pod.list(fcid)
  } map { case (deploys, services, pods) =>
    snapshot.FlinkK8sRefSnap(fcid.clusterId, fcid.namespace, deploys, services, pods)
  }

  override def viewLog(
      fcid: Fcid,
      podName: String,
      follow: Boolean,
      tailLines: Option[Int],
      sinceSec: Option[Int]): Stream[FlinkErr.K8sFailure, String] =
    k8sOperator
      .getPodLog(
        podName = podName,
        namespace = fcid.namespace,
        containerName = Some("flink-main-container"),
        follow = follow,
        tailLines = tailLines,
        sinceSec = sinceSec)
      .mapError(FlinkErr.K8sFailure.apply)
}
