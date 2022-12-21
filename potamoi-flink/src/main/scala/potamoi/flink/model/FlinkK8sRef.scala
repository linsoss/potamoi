package potamoi.flink.model

import potamoi.curTs
import potamoi.kubernetes.model.*
import zio.json.{DeriveJsonCodec, JsonCodec}

type K8sResourceName = String

/**
 * Flink referent kubernetes resource name listing.
 */
case class FlinkK8sRef(
    clusterId: String,
    namespace: String,
    deployment: List[K8sResourceName],
    service: List[K8sResourceName],
    pod: List[K8sResourceName],
    configMap: List[K8sResourceName]):
  lazy val fcid = Fcid(clusterId, namespace)

object FlinkK8sRef:
  given JsonCodec[FlinkK8sRef] = DeriveJsonCodec.gen[FlinkK8sRef]
  given Ordering[FlinkK8sRef]  = Ordering.by(e => (e.namespace, e.clusterId))

/**
 * Flink referent kubernetes resources snapshot.
 */
case class FlinkK8sRefSnap(
    clusterId: String,
    namespace: String,
    deployment: List[FlinkK8sDeploymentSnap],
    service: List[FlinkK8sServiceSnap],
    pod: List[FlinkK8sPodSnap]):
  lazy val fcid = Fcid(clusterId, namespace)

object FlinkK8sRefSnap:
  given JsonCodec[FlinkK8sRefSnap] = DeriveJsonCodec.gen[FlinkK8sRefSnap]
  given Ordering[FlinkK8sRefSnap]  = Ordering.by(e => (e.namespace, e.clusterId))

/**
 * Flink k8s component name that storage in k8s metadata.
 */
object FlK8sComponentName:
  val jobmanager  = "jobmanager"
  val taskmanager = "taskmanager"

/**
 * Flink k8s deployment resource info snapshot.
 * converter: [[potamoi.flink.observer.K8sEntityConverter#toDeploymentSnap]]
 */
case class FlinkK8sDeploymentSnap(
    clusterId: String,
    namespace: String,
    name: String,
    observedGeneration: Long,
    component: String,
    conditions: Vector[WorkloadCondition],
    replicas: Int,
    readyReplicas: Int,
    unavailableReplicas: Int,
    availableReplicas: Int,
    updatedReplicas: Int,
    createTime: Long,
    ts: Long = curTs):
  lazy val fcid = Fcid(clusterId, namespace)

object FlinkK8sDeploymentSnap:
  given JsonCodec[FlinkK8sDeploymentSnap] = DeriveJsonCodec.gen[FlinkK8sDeploymentSnap]

/**
 * Flink k8s service resource info snapshot.
 * converter: [[potamoi.flink.observer.K8sEntityConverter#toServiceSnap]]
 */
case class FlinkK8sServiceSnap(
    clusterId: String,
    namespace: String,
    name: String,
    component: String,
    dns: String,
    clusterIP: Option[String],
    ports: Set[SvcPort],
    isFlinkRestSvc: Boolean,
    createTime: Long,
    ts: Long = curTs):
  lazy val fcid = Fcid(clusterId, namespace)

case class SvcPort(name: String, protocol: String, port: Int, targetPort: Int)

object FlinkK8sServiceSnap:
  def apply(
      clusterId: String,
      namespace: String,
      name: String,
      component: String,
      clusterIP: Option[String],
      ports: Set[SvcPort],
      createTime: Long): FlinkK8sServiceSnap =
    FlinkK8sServiceSnap(
      clusterId = clusterId,
      namespace = namespace,
      name = name,
      component = component,
      clusterIP = clusterIP,
      ports = ports,
      createTime = createTime,
      dns = s"$name.$namespace",
      isFlinkRestSvc = name.endsWith("-rest"),
      ts = curTs
    )
  given JsonCodec[SvcPort]             = DeriveJsonCodec.gen[SvcPort]
  given JsonCodec[FlinkK8sServiceSnap] = DeriveJsonCodec.gen[FlinkK8sServiceSnap]

/**
 * Flink k8s pod resource info snapshot.
 * converter: [[potamoi.flink.observer.K8sEntityConverter#toPodSnap]]
 */
case class FlinkK8sPodSnap(
    clusterId: String,
    namespace: String,
    name: String,
    component: String,
    conditions: Vector[WorkloadCondition],
    phase: PodPhase,
    reason: Option[String],
    containerSnaps: Vector[PodContainerSnap],
    nodeName: String,
    hostIP: String,
    podIP: String,
    createTime: Long,
    startTime: Option[Long],
    ts: Long = curTs):
  lazy val fcid = Fcid(clusterId, namespace)

case class PodContainerSnap(
    name: String,
    image: String,
    ready: Boolean,
    restartCount: Int,
    state: ContainerState,
    stateDetail: ContainerStateDetail,
    cpuLimit: Option[K8sQuantity],
    cpuRequest: Option[K8sQuantity],
    memoryLimit: Option[K8sQuantity],
    memoryRequest: Option[K8sQuantity])

object FlinkK8sPodSnap:
  import potamoi.kubernetes.model.ContainerStates.given
  import potamoi.kubernetes.model.PodPhases.given
  given JsonCodec[PodContainerSnap] = DeriveJsonCodec.gen[PodContainerSnap]
  given JsonCodec[FlinkK8sPodSnap]  = DeriveJsonCodec.gen[FlinkK8sPodSnap]

/**
 * Flink k8s pod metrics.
 */
case class FlinkK8sPodMetrics(clusterId: String, namespace: String, name: String, metrics: PodMetrics):
  lazy val fcid = Fcid(clusterId, namespace)

object FlinkK8sPodMetrics:
  given JsonCodec[FlinkK8sPodMetrics] = DeriveJsonCodec.gen[FlinkK8sPodMetrics]
