package potamoi.flink.observer

import com.coralogix.zio.k8s.model.apps.v1.DeploymentSpec
import com.coralogix.zio.k8s.model.core.v1.{PodSpec, ServiceSpec}
import potamoi.flink.model.Fcid
import potamoi.flink.FlinkErr
import potamoi.flink.model.{FK8sDeploymentSnap, FK8sPodMetrics, FK8sPodSnap, FK8sServiceSnap, FlinkK8sRef, FlinkK8sRefSnap}
import zio.IO
import zio.stream.Stream

/**
 * Flink kubernetes resource snapshot information query layer.
 */
trait K8sRefQuery {

  def getRef(fcid: Fcid): IO[FlinkErr, Option[FlinkK8sRef]]
  def getRefSnapshot(fcid: Fcid): IO[FlinkErr, Option[FlinkK8sRefSnap]]
  def listRefs: Stream[FlinkErr, FlinkK8sRef]
  def listRefSnapshots: Stream[FlinkErr, FlinkK8sRefSnap]

  def listDeploymentSnaps(fcid: Fcid): IO[FlinkErr, List[FK8sDeploymentSnap]]
  def listServiceSnaps(fcid: Fcid): IO[FlinkErr, List[FK8sServiceSnap]]
  def listPodSnaps(fcid: Fcid): IO[FlinkErr, List[FK8sPodSnap]]
  def listContainerNames(fcid: Fcid, podName: String): IO[FlinkErr, List[String]]

  def getDeploymentSnap(fcid: Fcid, deployName: String): IO[FlinkErr, Option[FK8sDeploymentSnap]]
  def getServiceSnap(fcid: Fcid, svcName: String): IO[FlinkErr, Option[FK8sServiceSnap]]
  def getPodSnap(fcid: Fcid, podName: String): IO[FlinkErr, Option[FK8sPodSnap]]
  def getConfigMapNames(fcid: Fcid): IO[FlinkErr, List[String]]

  def getDeploymentSpec(fcid: Fcid, deployName: String): IO[FlinkErr, Option[DeploymentSpec]]
  def getServiceSpec(fcid: Fcid, deployName: String): IO[FlinkErr, Option[ServiceSpec]]
  def getPodSpec(fcid: Fcid, deployName: String): IO[FlinkErr, Option[PodSpec]]
  def getConfigMapData(fcid: Fcid, configMapName: String): IO[FlinkErr, Map[String, String]]
  def listConfigMapData(fcid: Fcid): IO[FlinkErr, Map[String, String]]

  def getPodMetrics(fcid: Fcid, podName: String): IO[FlinkErr, Option[FK8sPodMetrics]]
  def listPodMetrics(fcid: Fcid): IO[FlinkErr, List[FK8sPodMetrics]]

  /**
   * Only for getting flink-main-container logs, side-car container logs should be obtained
   * through the [[K8sOperator.getPodLog()]].
   */
  def getLog(
      fcid: Fcid,
      podName: String,
      follow: Boolean = false,
      tailLines: Option[Int] = None,
      sinceSec: Option[Int] = None): Stream[FlinkErr, String]
}
