package potamoi.flink.observer.tracker

import com.coralogix.zio.k8s.client.model.{label, Added, Deleted, K8sNamespace, Modified, Reseted}
import com.coralogix.zio.k8s.client.K8sFailure
import com.devsisters.shardcake.*
import potamoi.common.Syntax.toPrettyString
import potamoi.flink.FlinkConf
import potamoi.flink.model.{Fcid, FlinkK8sPodMetrics, FlinkK8sServiceSnap, FlinkRestSvcEndpoint}
import potamoi.flink.observer.tracker.K8sEntityConverter.*
import potamoi.flink.storage.FlinkSnapshotStorage
import potamoi.kubernetes.{K8sClient, K8sOperator}
import potamoi.kubernetes.K8sErr.PodNotFound
import potamoi.syntax.valueToSome
import potamoi.times.given_Conversion_ScalaDuration_ZioDuration
import zio.*
import zio.stream.ZStream
import zio.Schedule.{recurWhile, spaced, succeed}
import zio.ZIO.{logError, logInfo, logWarning}
import zio.ZIOAspect.annotated

import scala.collection.mutable

/**
 * Flink kubernetes ref resource snapshot tracker.
 */
object FlinkK8sRefTracker {

  object Entity extends EntityType[Cmd]("flinkK8sRefTracker")

  sealed trait Cmd
  case class Start(replier: Replier[Ack.type])    extends Cmd
  case class Stop(replier: Replier[Ack.type])     extends Cmd
  case object Terminate                           extends Cmd
  case class IsStarted(replier: Replier[Boolean]) extends Cmd

  // noinspection DuplicatedCode
  private case class TrackerState(isStarted: Ref[Boolean], trackTaskFibersRef: Ref[Set[Fiber.Runtime[_, _]]]):
    def reset: UIO[Unit] =
      for {
        _ <- trackTaskFibersRef.getAndSet(Set.empty).flatMap { fibers =>
               ZIO.foreachPar(fibers)(_.interrupt)
             }
        _ <- isStarted.set(false)
      } yield ()

  private object TrackerState:
    def make: UIO[TrackerState] =
      for {
        isStarted          <- Ref.make[Boolean](false)
        trackTaskFibersRef <- Ref.make[Set[Fiber.Runtime[_, _]]](Set.empty)
      } yield TrackerState(isStarted, trackTaskFibersRef)
}

class FlinkK8sRefTracker(flinkConf: FlinkConf, snapStore: FlinkSnapshotStorage, k8sOperator: K8sOperator) {

  import FlinkK8sRefTracker.*
  private val watchEffectRecoverInterval = 1.seconds
  private val watchEffectClock           = ZLayer.succeed(Clock.ClockLive)

  /**
   * Sharding entity behavior.
   */
  def behavior(entityId: String, messages: Dequeue[Cmd]): RIO[Sharding with Scope, Nothing] =
    for {
      fcid   <- ZIO.attempt(unmarshallFcid(entityId)).tapErrorCause(cause => ZIO.logErrorCause(s"Fail to unmarshall Fcid: entityId", cause))
      state  <- TrackerState.make
      // start tracking automatically if needs.
      _      <- snapStore.trackedList
                  .exists(fcid)
                  .catchAll(_ => ZIO.succeed(false))
                  .flatMap { shouldAutoStart =>
                    if shouldAutoStart then start(fcid, state) else ZIO.unit
                  }
                  .forkScoped
      effect <- messages.take.flatMap(handleMessage(fcid, _, state)).forever
    } yield effect

  private def handleMessage(fcid: Fcid, message: Cmd, state: TrackerState): RIO[Sharding with Scope, Unit] = {
    message match {
      case Start(replier)     => start(fcid, state) *> replier.reply(Ack)
      case Stop(replier)      => logInfo(s"Flink k8s refs tracker stopped: ${fcid.show}") *> state.reset *> replier.reply(Ack)
      case Terminate          => logInfo(s"Flink k8s refs tracker terminated: ${fcid.show}") *> state.reset
      case IsStarted(replier) => state.isStarted.get.flatMap(replier.reply)
    }
  } @@ ZIOAspect.annotated(fcid.toAnno*)

  private def start(fcid: Fcid, state: TrackerState) = {
    state.isStarted.get.flatMap {
      case true  => ZIO.unit
      case false =>
        for {
          _                   <- logInfo(s"Flink k8s refs tracker started: ${fcid.show}")
          _                   <- state.reset
          watchDeployFiber    <- watchDeployments(fcid).forkScoped
          watchSvcFiber       <- watchServices(fcid).forkScoped
          watchPodFiber       <- watchPods(fcid).forkScoped
          watchConfigmapFiber <- watchConfigmapNames(fcid).forkScoped
          pollPodMetricsFiber <- pollPodMetrics(fcid).forkScoped
          _                   <- state.trackTaskFibersRef.set(Set(watchDeployFiber, watchSvcFiber, watchPodFiber, watchConfigmapFiber, pollPodMetricsFiber))
          _                   <- state.isStarted.set(true)
        } yield ()
    }
  }

  private def appSelector(fcid: Fcid) = label("app") === fcid.clusterId && label("type") === "flink-native-kubernetes"

  /**
   * Auto retry k8s watching effect when it fails, and log the first different error.
   */
  extension [E](k8sWatchEffect: ZIO[Clock, E, Unit])
    inline def autoRetry: UIO[Unit] = Ref.make[Option[E]](None).flatMap { errState =>
      k8sWatchEffect
        .tapError { err =>
          errState.get.flatMap { pre =>
            (logError(err match
              case e: Throwable => e.getMessage
              case e            => toPrettyString(e)
            ) *> errState.set(Some(err))).when(!pre.contains(err))
          }
        }
        .retry(Schedule.spaced(watchEffectRecoverInterval))
        .ignore
        .provide(watchEffectClock)
    }

  /**
   * Watch k8s deployments api.
   */
  def watchDeployments(fcid: Fcid): UIO[Unit] = k8sOperator.client.flatMap { client =>
    client.deployments
      .watchForever(namespace = K8sNamespace(fcid.namespace), labelSelector = appSelector(fcid))
      .runForeach {
        case Reseted()        => snapStore.k8sRef.deployment.rm(fcid)
        case Added(deploy)    => toDeploymentSnap(deploy).flatMap(snapStore.k8sRef.deployment.put)
        case Modified(deploy) => toDeploymentSnap(deploy).flatMap(snapStore.k8sRef.deployment.put)
        case Deleted(deploy)  => toDeploymentSnap(deploy).flatMap(e => snapStore.k8sRef.deployment.rm(e.fcid, e.name))
      }
      .autoRetry
  }

  /**
   * Watch k8s services api.
   */
  def watchServices(fcid: Fcid): UIO[Unit] = k8sOperator.client.flatMap { client =>
    client.services
      .watchForever(namespace = K8sNamespace(fcid.namespace), labelSelector = appSelector(fcid))
      .runForeach {
        case Reseted()     => snapStore.k8sRef.service.rm(fcid)
        case Added(svc)    => toServiceSnap(svc).flatMap(e => snapStore.k8sRef.service.put(e) <&> saveRestEndpoint(e))
        case Modified(svc) => toServiceSnap(svc).flatMap(e => snapStore.k8sRef.service.put(e) <&> saveRestEndpoint(e))
        case Deleted(svc)  => toServiceSnap(svc).flatMap(e => snapStore.k8sRef.service.rm(e.fcid, e.name) <&> rmRestEndpoint(e))
      }
      .autoRetry
  }

  private def saveRestEndpoint(svc: FlinkK8sServiceSnap) =
    ZIO
      .succeed(svc)
      .flatMap { e =>
        FlinkRestSvcEndpoint.of(e) match
          case None      => ZIO.unit
          case Some(ept) => snapStore.restEndpoint.put(e.fcid, ept)
      }
      .unit
      .when(svc.isFlinkRestSvc)

  private def rmRestEndpoint(svc: FlinkK8sServiceSnap) = snapStore.restEndpoint.rm(svc.fcid).when(svc.isFlinkRestSvc)

  /**
   * Watch k8s pods api.
   */
  def watchPods(fcid: Fcid): UIO[Unit] = k8sOperator.client.flatMap { client =>
    client.pods
      .watchForever(namespace = K8sNamespace(fcid.namespace), labelSelector = appSelector(fcid))
      .runForeach {
        case Reseted()     => snapStore.k8sRef.pod.rm(fcid)
        case Added(pod)    => toPodSnap(pod).flatMap(snapStore.k8sRef.pod.put)
        case Modified(pod) => toPodSnap(pod).flatMap(snapStore.k8sRef.pod.put)
        case Deleted(pod)  => toPodSnap(pod).flatMap(e => snapStore.k8sRef.pod.rm(e.fcid, e.name))
      }
      .autoRetry
  }

  /**
   * Watch k8s configmap api.
   */
  def watchConfigmapNames(fcid: Fcid): UIO[Unit] = k8sOperator.client.flatMap { client =>
    client.configMaps
      .watchForever(namespace = K8sNamespace(fcid.namespace), labelSelector = appSelector(fcid))
      .runForeach {
        case Reseted()           => snapStore.k8sRef.configmap.rm(fcid)
        case Added(configMap)    => configMap.getName.flatMap(snapStore.k8sRef.configmap.put(fcid, _))
        case Modified(configMap) => configMap.getName.flatMap(snapStore.k8sRef.configmap.put(fcid, _))
        case Deleted(configMap)  => configMap.getName.flatMap(snapStore.k8sRef.configmap.rm(fcid, _))
      }
      .autoRetry
  }

  /**
   * Poll k8s pod metrics api.
   */
  def pollPodMetrics(fcid: Fcid): UIO[Unit] = {

    def watchPodNames(podNames: Ref[mutable.HashSet[String]]) = k8sOperator.client.flatMap { client =>
      client.pods
        .watchForever(namespace = K8sNamespace(fcid.namespace), labelSelector = appSelector(fcid))
        .runForeach {
          case Reseted()     => podNames.set(mutable.HashSet.empty) *> snapStore.k8sRef.podMetrics.rm(fcid).ignore
          case Added(pod)    => pod.getMetadata.flatMap(_.getName).flatMap(n => podNames.update(_ += n))
          case Modified(pod) => pod.getMetadata.flatMap(_.getName).flatMap(n => podNames.update(_ += n))
          case Deleted(pod)  => pod.getMetadata.flatMap(_.getName).flatMap(n => podNames.update(_ -= n) *> snapStore.k8sRef.podMetrics.rm(fcid, n))
        }
        .autoRetry
    }

    def pollingMetricsApi(podNames: Ref[mutable.HashSet[String]]) = ZStream
      .fromIterableZIO(podNames.get)
      .mapZIOParUnordered(5) { name =>
        k8sOperator
          .getPodMetrics(name, fcid.namespace)
          .map(metrics => Some(FlinkK8sPodMetrics(fcid.clusterId, fcid.namespace, name, metrics.copy())))
          .catchSome { case PodNotFound(_, _) => ZIO.succeed(None) }
      }
      .runForeach(m => snapStore.k8sRef.podMetrics.put(m.get).when(m.isDefined))

    for {
      podNames <- Ref.make(mutable.HashSet.empty[String])
      _        <- watchPodNames(podNames).fork
      pollProc <- loopTrigger(flinkConf.tracking.k8sPodMetricsPolling, pollingMetricsApi(podNames))(using flinkConf.tracking.logTrackersFailedInfo)
    } yield pollProc
  }

}
