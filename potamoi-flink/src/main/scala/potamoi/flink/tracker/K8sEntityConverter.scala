package potamoi.flink.tracker

import com.coralogix.zio.k8s.model.apps.v1.{Deployment, DeploymentCondition}
import com.coralogix.zio.k8s.model.core.v1.{Container, ContainerStatus, Pod, PodCondition, Service, ServicePort}
import potamoi.flink.K8sEntityConvertErr.*
import potamoi.flink.model.{FlinkK8sDeploymentSnap, FlinkK8sPodSnap, FlinkK8sServiceSnap, PodContainerSnap, SvcPort}
import potamoi.curTs
import potamoi.kubernetes.*
import potamoi.kubernetes.model.*
import zio.{IO, ZIO}
import zio.ZIO.{fail, succeed}
import zio.prelude.data.Optional
import zio.prelude.data.Optional.{Absent, Present}

import scala.util.Try

/**
 * Convert raw kubernetes entity to flink k8s ref entity.
 */
object K8sEntityConverter {

  extension [E, A](io: IO[Throwable, Optional[A]]) {
    def dry(sideEf: => E): IO[E, A] = io.orElseFail(sideEf).flatMap {
      case Present(a) => succeed(a)
      case Absent     => fail(sideEf)
    }
  }

  /**
   * Convert [[Service]] to [[FlinkK8sServiceSnap]].
   */
  def toServiceSnap(svc: Service): IO[IllegalK8sServiceEntity.type, FlinkK8sServiceSnap] = ZIO
    .attempt {
      for {
        metadata        <- svc.metadata
        namespace       <- metadata.namespace
        name            <- metadata.name
        createTimestamp <- metadata.creationTimestamp.map(_.value.toEpochSecond)
        spec            <- svc.spec
        selector        <- spec.selector
        app             <- selector.get("app").toOptional
        component       <- selector.get("component").toOptional
        clusterIP = spec.clusterIP.toOption
        ports = spec.ports
          .getOrElse(Vector.empty[ServicePort])
          .map { svcPort =>
            for {
              portName <- svcPort.name
              protocol <- svcPort.protocol
              targetPort <- svcPort.targetPort.map(_.value match {
                case Left(v)  => v
                case Right(v) => v.toInt
              })
              port = svcPort.port
            } yield SvcPort(
              name = portName,
              protocol = protocol,
              port = port,
              targetPort = targetPort
            )
          }
          .filter(_.isDefined)
          .map(_.toOption.get)
          .toSet
      } yield FlinkK8sServiceSnap(
        clusterId = app,
        namespace = namespace,
        name = name,
        component = component,
        clusterIP = clusterIP,
        ports = ports,
        createTime = createTimestamp
      )
    }
    .dry(IllegalK8sServiceEntity)

  /**
   * Convert [[Deployment]] to [[FlinkK8sDeploymentSnap]].
   */
  // noinspection DuplicatedCode
  def toDeploymentSnap(deploy: Deployment): IO[IllegalK8sDeploymentEntity.type, FlinkK8sDeploymentSnap] = ZIO
    .attempt {
      for {
        metadata        <- deploy.metadata
        namespace       <- metadata.namespace
        name            <- metadata.name
        createTimestamp <- metadata.creationTimestamp.map(_.value.toEpochSecond)
        spec            <- deploy.spec
        selector        <- spec.selector.matchLabels
        app             <- selector.get("app").toOptional
        component       <- selector.get("component").toOptional
        status          <- deploy.status
        observedGeneration  = status.observedGeneration.getOrElse(0L)
        replicas            = status.replicas.getOrElse(0)
        readyReplicas       = status.readyReplicas.getOrElse(0)
        unavailableReplicas = status.unavailableReplicas.getOrElse(0)
        availableReplicas   = status.availableReplicas.getOrElse(0)
        updatedReplicas     = status.updatedReplicas.getOrElse(0)
        conditions = status.conditions
          .getOrElse(Vector.empty[DeploymentCondition])
          .map { cond =>
            WorkloadCondition(
              condType = cond.`type`,
              status = Try(WorkloadCondStatus.valueOf(cond.status)).getOrElse(WorkloadCondStatus.Unknown),
              reason = cond.reason.toOption,
              message = cond.message.toOption,
              lastTransitionTime = cond.lastTransitionTime.map(_.value.toEpochSecond).toOption
            )
          }
          .sorted
          .reverse
      } yield FlinkK8sDeploymentSnap(
        clusterId = app,
        namespace = namespace,
        name = name,
        component = component,
        observedGeneration = observedGeneration,
        conditions = conditions,
        replicas = replicas,
        readyReplicas = readyReplicas,
        unavailableReplicas = unavailableReplicas,
        availableReplicas = availableReplicas,
        updatedReplicas = updatedReplicas,
        createTime = createTimestamp
      )
    }
    .dry(IllegalK8sDeploymentEntity)

  /**
   * Convert [[Pod]] to [[FK8sPodSnap]].
   */
  // noinspection DuplicatedCode
  def toPodSnap(pod: Pod): IO[IllegalK8sPodEntity.type, FlinkK8sPodSnap] = ZIO
    .attempt {
      for {
        metadata        <- pod.metadata
        namespace       <- metadata.namespace
        name            <- metadata.name
        createTimestamp <- metadata.creationTimestamp.map(_.value.toEpochSecond)
        labels = metadata.labels.getOrElse(Map.empty[String, String])
        app       <- labels.get("app").toOptional
        component <- labels.get("component").toOptional

        spec <- pod.spec
        nodeName = spec.nodeName.getOrElse("")
        containerResources = spec.containers
          .getOrElse(Vector.empty[Container])
          .map { container =>
            val name = container.name
            val (cpuLimit, memLimit, cpuReq, memReq) = container.resources match {
              case Absent => (None, None, None, None)
              case Present(rs) =>
                val cpuL = rs.limits.map(_.get("cpu").map(e => K8sQuantity(e.value))).flatten
                val memL = rs.limits.map(_.get("memory").map(e => K8sQuantity(e.value))).flatten
                val couR = rs.requests.map(_.get("cpu").map(e => K8sQuantity(e.value))).flatten
                val memR = rs.requests.map(_.get("memory").map(e => K8sQuantity(e.value))).flatten
                (cpuL, memL, couR, memR)
            }
            name -> (cpuLimit, memLimit, cpuReq, memReq)
          }
          .toMap

        status <- pod.status
        conditions = status.conditions
          .getOrElse(Vector.empty[PodCondition])
          .map { cond =>
            WorkloadCondition(
              condType = cond.`type`,
              status = Try(WorkloadCondStatus.valueOf(cond.status)).getOrElse(WorkloadCondStatus.Unknown),
              reason = cond.reason.toOption,
              message = cond.message.toOption,
              lastTransitionTime = cond.lastTransitionTime.map(_.value.toEpochSecond).toOption
            )
          }
          .sorted
          .reverse
        phase     = Try(PodPhase.valueOf(status.phase.getOrElse(""))).getOrElse(PodPhase.Unknown)
        reason    = status.reason.toOption
        hostIP    = status.hostIP.toOption.getOrElse("")
        podIP     = status.podIP.toOption.getOrElse("")
        startTime = status.startTime.map(_.value.toEpochSecond).toOption
        containerSnaps = status.containerStatuses
          .getOrElse(Vector.empty[ContainerStatus])
          .map { container =>
            val name = container.name
            val (state, stateDetail) = container.state match {
              case Absent     => ContainerState.Unknown -> ContainerStateUnknown
              case Present(s) => ContainerStateDetail.resolve(s)
            }
            val (cpuLimit, memLimit, cpuReq, memReq) = containerResources.getOrElse(name, (None, None, None, None))
            PodContainerSnap(
              name = name,
              image = container.image,
              ready = container.ready,
              restartCount = container.restartCount,
              state = state,
              stateDetail = stateDetail,
              cpuLimit = cpuLimit,
              cpuRequest = cpuReq,
              memoryLimit = memLimit,
              memoryRequest = memReq
            )
          }
      } yield FlinkK8sPodSnap(
        clusterId = app,
        namespace = namespace,
        name = name,
        component = component,
        conditions = conditions,
        phase = phase,
        reason = reason,
        containerSnaps = containerSnaps,
        nodeName = nodeName,
        hostIP = hostIP,
        podIP = podIP,
        createTime = createTimestamp,
        startTime = startTime,
        ts = curTs
      )
    }
    .dry(IllegalK8sPodEntity)
}
