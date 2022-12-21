package potamoi.flink.storage

import potamoi.flink.{DataStorageErr, JobId}
import potamoi.flink.model.*
import zio.{IO, ZIO}
import zio.stream.Stream

/**
 * Flink k8s resource snapshot storage.
 */
trait K8sRefSnapStorage:
  def deployment: K8sDeploymentSnapStorage
  def service: K8sServiceSnapStorage
  def pod: K8sPodSnapStorage
  def podMetrics: K8sPodMetricsStorage
  def configmap: K8sConfigmapNamesStorage

/**
 * Storage for flink k8s deployment snapshot.
 */
trait K8sDeploymentSnapStorage extends K8sDeploymentSnapStorage.Modify with K8sDeploymentSnapStorage.Query

object K8sDeploymentSnapStorage {
  trait Modify:
    def put(snap: FlinkK8sDeploymentSnap): IO[DataStorageErr, Unit]
    def rm(fcid: Fcid, deployName: String): IO[DataStorageErr, Unit]
    def rm(fcid: Fcid): IO[DataStorageErr, Unit]

  trait Query:
    def get(fcid: Fcid, deployName: String): IO[DataStorageErr, Option[FlinkK8sDeploymentSnap]]
    def list(fcid: Fcid): IO[DataStorageErr, List[FlinkK8sDeploymentSnap]]
    def listName(fcid: Fcid): IO[DataStorageErr, List[String]]
}

/**
 * Storage for flink k8s service snapshot.
 */
trait K8sServiceSnapStorage extends K8sServiceSnapStorage.Modify with K8sServiceSnapStorage.Query

object K8sServiceSnapStorage {
  trait Modify:
    def put(snap: FlinkK8sServiceSnap): IO[DataStorageErr, Unit]
    def rm(fcid: Fcid, svcName: String): IO[DataStorageErr, Unit]
    def rm(fcid: Fcid): IO[DataStorageErr, Unit]

  trait Query:
    def get(fcid: Fcid, svcName: String): IO[DataStorageErr, Option[FlinkK8sServiceSnap]]
    def list(fcid: Fcid): IO[DataStorageErr, List[FlinkK8sServiceSnap]]
    def listName(fcid: Fcid): IO[DataStorageErr, List[String]]
}

/**
 * Storage for flink k8s pods snapshot.
 */
trait K8sPodSnapStorage extends K8sPodSnapStorage.Modify with K8sPodSnapStorage.Query

object K8sPodSnapStorage {
  trait Modify:
    def put(snap: FlinkK8sPodSnap): IO[DataStorageErr, Unit]
    def rm(fcid: Fcid, podName: String): IO[DataStorageErr, Unit]
    def rm(fcid: Fcid): IO[DataStorageErr, Unit]

  trait Query:
    def get(fcid: Fcid, podName: String): IO[DataStorageErr, Option[FlinkK8sPodSnap]]
    def list(fcid: Fcid): IO[DataStorageErr, List[FlinkK8sPodSnap]]
    def listName(fcid: Fcid): IO[DataStorageErr, List[String]]
}

/**
 * Storage for flink k8s pods metrics.
 */
trait K8sPodMetricsStorage extends K8sPodMetricsStorage.Modify with K8sPodMetricsStorage.Query

object K8sPodMetricsStorage {
  trait Modify:
    def put(snap: FlinkK8sPodMetrics): IO[DataStorageErr, Unit]
    def rm(fcid: Fcid, podName: String): IO[DataStorageErr, Unit]
    def rm(fcid: Fcid): IO[DataStorageErr, Unit]

  trait Query:
    def get(fcid: Fcid, podName: String): IO[DataStorageErr, Option[FlinkK8sPodMetrics]]
    def list(fcid: Fcid): IO[DataStorageErr, List[FlinkK8sPodMetrics]]
    def listName(fcid: Fcid): IO[DataStorageErr, List[String]]
}

/**
 * Storage for flink k8s configMap names.
 */
trait K8sConfigmapNamesStorage extends K8sConfigmapNamesStorage.Modify with K8sConfigmapNamesStorage.Query

object K8sConfigmapNamesStorage {
  trait Modify:
    def put(fcid: Fcid, configmapName: String): IO[DataStorageErr, Unit]
    def rm(fcid: Fcid, configmapName: String): IO[DataStorageErr, Unit]
    def rm(fcid: Fcid): IO[DataStorageErr, Unit]

  trait Query:
    def listName(fcid: Fcid): IO[DataStorageErr, List[String]]
}
