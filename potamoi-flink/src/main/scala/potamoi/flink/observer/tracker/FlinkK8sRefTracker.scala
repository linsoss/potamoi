package potamoi.flink.observer.tracker

import akka.actor.typed.{ActorRef, Behavior, PostStop, SupervisorStrategy}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import com.coralogix.zio.k8s.client.model.{label, Added, Deleted, K8sNamespace, Modified, Reseted}
import potamoi.{KryoSerializable, NodeRoles}
import potamoi.akka.actors.*
import potamoi.akka.behaviors.*
import potamoi.akka.zios.*
import potamoi.akka.ShardingProxy
import potamoi.flink.FlinkConf
import potamoi.flink.model.Fcid
import potamoi.flink.model.snapshot.{FlinkK8sPodMetrics, FlinkK8sServiceSnap, FlinkRestSvcEndpoint}
import potamoi.flink.observer.tracker.K8sEntityConverter.*
import potamoi.flink.storage.FlinkDataStorage
import potamoi.kubernetes.{K8sClient, K8sOperator}
import potamoi.kubernetes.K8sErr.PodNotFound
import potamoi.logger.LogConf
import potamoi.options.unsafeGet
import potamoi.syntax.{toPrettyString, valueToSome}
import potamoi.times.{given_Conversion_ScalaDuration_ZIODuration, given_Conversion_ZIODuration_ScalaDuration}
import zio.*
import zio.stream.ZStream
import zio.Schedule.{recurWhile, spaced, succeed}
import zio.ZIO.{logError, logInfo, logWarning}
import zio.ZIOAspect.annotated

import scala.collection.mutable
import scala.util.{Failure, Success}

/**
 * Flink kubernetes ref resource snapshot tracker.
 */
object FlinkK8sRefTracker extends ShardingProxy[Fcid, FlinkK8sRefTrackerActor.Cmd] {

  private var logConfInstance: Option[LogConf]                 = None
  private var flinkConfInstance: Option[FlinkConf]             = None
  private var flinkDataStoreInstance: Option[FlinkDataStorage] = None
  private var k8sOperatorInstance: Option[K8sOperator]         = None

  private[tracker] def unsafeLogConf: Option[LogConf]                 = logConfInstance
  private[tracker] def unsafeFlinkConf: Option[FlinkConf]             = flinkConfInstance
  private[tracker] def unsafeFlinkDataStore: Option[FlinkDataStorage] = flinkDataStoreInstance
  private[tracker] def unsafeK8sOperator: Option[K8sOperator]         = k8sOperatorInstance

  /**
   * Actor behavior.
   */
  // noinspection DuplicatedCode
  def apply(logConf: LogConf, flinkConf: FlinkConf, dataStore: FlinkDataStorage, k8sOperator: K8sOperator): Behavior[Req] = {
    logConfInstance = Some(logConf)
    flinkConfInstance = Some(flinkConf)
    flinkDataStoreInstance = Some(dataStore)
    k8sOperatorInstance = Some(k8sOperator)
    behavior(
      entityKey = EntityTypeKey[FlinkK8sRefTrackerActor.Cmd]("flink-k8s-tracker"),
      marshallKey = marshallFcid,
      unmarshallKey = unmarshallFcid,
      createBehavior = fcid => FlinkK8sRefTrackerActor(fcid),
      stopMessage = Some(FlinkK8sRefTrackerActor.Terminate),
      bindRole = Some(NodeRoles.flinkService)
    )
  }
}

/**
 * Flink kubernetes ref resource snapshot tracker sharding actor.
 */
object FlinkK8sRefTrackerActor {

  sealed trait Cmd                                       extends KryoSerializable
  final case class Start(replier: ActorRef[Ack.type])    extends Cmd
  final case class Stop(replier: ActorRef[Ack.type])     extends Cmd
  case object Terminate                                  extends Cmd
  final case class IsStarted(replier: ActorRef[Boolean]) extends Cmd
  private case class ShouldAutoStart(rs: Boolean)        extends Cmd

  def apply(fcid: Fcid): Behavior[Cmd] =
    Behaviors.setup { ctx =>
      new FlinkK8sRefTrackerActor(fcid)(using ctx).active.onFailure[Exception](defaultTrackerFailoverStrategy)
    }
}

import potamoi.flink.observer.tracker.FlinkK8sRefTrackerActor.*

class FlinkK8sRefTrackerActor(fcid: Fcid)(using ctx: ActorContext[Cmd]) {

  private given LogConf   = FlinkK8sRefTracker.unsafeLogConf.get
  private val flinkConf   = FlinkK8sRefTracker.unsafeFlinkConf.unsafeGet
  private val dataStore   = FlinkK8sRefTracker.unsafeFlinkDataStore.unsafeGet
  private val k8sOperator = FlinkK8sRefTracker.unsafeK8sOperator.unsafeGet

  private val watchEffectRecoverInterval = 1.seconds
  private val watchEffectClock           = ZLayer.succeed(Clock.ClockLive)
  private def appSelector(fcid: Fcid)    = label("app") === fcid.clusterId && label("type") === "flink-native-kubernetes"

  // actor state
  private var isStarted                                = false
  private var workProc: Option[CancelableFuture[Unit]] = None

  /**
   * Actor start behavior.
   */
  // noinspection DuplicatedCode
  def start: Behavior[Cmd] = Behaviors.withStash(100) { stash =>
    Behaviors
      .receiveMessage[Cmd] {
        case ShouldAutoStart(r) =>
          if r then ctx.self ! Start(ctx.system.ignoreRef)
          stash.unstashAll(active)
        case cmd                =>
          stash.stash(cmd)
          Behaviors.same
      }
      .beforeIt {
        ctx.pipeToSelf(dataStore.trackedList.exists(fcid).runAsync) {
          case Success(r) => ShouldAutoStart(r)
          case Failure(e) => ShouldAutoStart(false)
        }
      }
  }

  /**
   * Actor active behavior.
   */
  def active: Behavior[Cmd] = Behaviors
    .receiveMessagePartial[Cmd] {

      case Start(reply) =>
        if (!isStarted) {
          workProc.map(_.cancel())
          workProc = Some(launchTrackers.runAsync)
          isStarted = true
          ctx.log.info(s"Flink k8s refs tracker started: ${fcid.show}")
        }
        reply ! Ack
        Behaviors.same

      case Stop(reply) =>
        workProc.map(_.cancel())
        isStarted = false
        reply ! Ack
        Behaviors.stopped

      case Terminate =>
        workProc.map(_.cancel())
        isStarted = false
        Behaviors.stopped

      case IsStarted(reply) =>
        reply ! isStarted
        Behaviors.same
    }
    .receiveSignal { case (_, PostStop) =>
      ctx.log.info(s"Flink k8s refs tracker stopped: ${fcid.show}")
      workProc.map(_.cancel())
      Behaviors.same
    }

  /**
   * Launch all flink kubernetes info trackers.
   */
  private def launchTrackers: UIO[Unit] = {
    for {
      _ <- watchDeployments.fork
      _ <- watchServices.fork
      _ <- watchPods.fork
      _ <- watchConfigmapNames.fork
      _ <- pollPodMetrics.fork
      _ <- ZIO.never
    } yield ()
  } @@ annotated(fcid.toAnno*)

  /**
   * Auto retry k8s watching effect when it fails, and log the first different error.
   */
  extension [E](k8sWatchEffect: ZIO[Clock, E, Unit])
    inline def autoRetry: UIO[Unit] = Ref.make[Option[E]](None).flatMap { errState =>
      k8sWatchEffect
        .tapError { err =>
          errState.get.flatMap { pre =>
            (
              logError(err match
                case e: Throwable => e.getMessage
                case e            => toPrettyString(e)
              ) *> errState.set(Some(err))
            ).when(!pre.contains(err))
          }
        }
        .retry(Schedule.spaced(watchEffectRecoverInterval))
        .ignore
        .provide(watchEffectClock)
    }

  /**
   * Watch k8s deployments api.
   */
  def watchDeployments: UIO[Unit] = {
    k8sOperator.client.deployments
      .watchForever(namespace = K8sNamespace(fcid.namespace), labelSelector = appSelector(fcid))
      .runForeach {
        case Reseted()        => dataStore.k8sRef.deployment.rm(fcid)
        case Added(deploy)    => toDeploymentSnap(deploy).flatMap(dataStore.k8sRef.deployment.put)
        case Modified(deploy) => toDeploymentSnap(deploy).flatMap(dataStore.k8sRef.deployment.put)
        case Deleted(deploy)  => toDeploymentSnap(deploy).flatMap(e => dataStore.k8sRef.deployment.rm(e.fcid, e.name))
      }
      .autoRetry
  }

  /**
   * Watch k8s services api.
   */
  def watchServices: UIO[Unit] = {
    k8sOperator.client.services
      .watchForever(namespace = K8sNamespace(fcid.namespace), labelSelector = appSelector(fcid))
      .runForeach {
        case Reseted()     => dataStore.k8sRef.service.rm(fcid)
        case Added(svc)    => toServiceSnap(svc).flatMap(e => dataStore.k8sRef.service.put(e) <&> saveRestEndpoint(e))
        case Modified(svc) => toServiceSnap(svc).flatMap(e => dataStore.k8sRef.service.put(e) <&> saveRestEndpoint(e))
        case Deleted(svc)  => toServiceSnap(svc).flatMap(e => dataStore.k8sRef.service.rm(e.fcid, e.name) <&> rmRestEndpoint(e))
      }
      .autoRetry
  }

  private def saveRestEndpoint(svc: FlinkK8sServiceSnap) =
    ZIO
      .succeed(svc)
      .flatMap { e =>
        FlinkRestSvcEndpoint.of(e) match
          case None      => ZIO.unit
          case Some(ept) => dataStore.restEndpoint.put(e.fcid, ept)
      }
      .unit
      .when(svc.isFlinkRestSvc)

  private def rmRestEndpoint(svc: FlinkK8sServiceSnap) = dataStore.restEndpoint.rm(svc.fcid).when(svc.isFlinkRestSvc)

  /**
   * Watch k8s pods api.
   */
  def watchPods: UIO[Unit] = {
    k8sOperator.client.pods
      .watchForever(namespace = K8sNamespace(fcid.namespace), labelSelector = appSelector(fcid))
      .runForeach {
        case Reseted()     => dataStore.k8sRef.pod.rm(fcid)
        case Added(pod)    => toPodSnap(pod).flatMap(dataStore.k8sRef.pod.put)
        case Modified(pod) => toPodSnap(pod).flatMap(dataStore.k8sRef.pod.put)
        case Deleted(pod)  => toPodSnap(pod).flatMap(e => dataStore.k8sRef.pod.rm(e.fcid, e.name))
      }
      .autoRetry
  }

  /**
   * Watch k8s configmap api.
   */
  def watchConfigmapNames: UIO[Unit] = {
    k8sOperator.client.configMaps
      .watchForever(namespace = K8sNamespace(fcid.namespace), labelSelector = appSelector(fcid))
      .runForeach {
        case Reseted()           => dataStore.k8sRef.configmap.rm(fcid)
        case Added(configMap)    => configMap.getName.flatMap(dataStore.k8sRef.configmap.put(fcid, _))
        case Modified(configMap) => configMap.getName.flatMap(dataStore.k8sRef.configmap.put(fcid, _))
        case Deleted(configMap)  => configMap.getName.flatMap(dataStore.k8sRef.configmap.rm(fcid, _))
      }
      .autoRetry
  }

  /**
   * Poll k8s pod metrics api.
   */
  def pollPodMetrics: UIO[Unit] = {

    def watchPodNames(podNames: Ref[mutable.HashSet[String]]) = {
      k8sOperator.client.pods
        .watchForever(namespace = K8sNamespace(fcid.namespace), labelSelector = appSelector(fcid))
        .runForeach {
          case Reseted()     => podNames.set(mutable.HashSet.empty) *> dataStore.k8sRef.podMetrics.rm(fcid).ignore
          case Added(pod)    => pod.getMetadata.flatMap(_.getName).flatMap(n => podNames.update(_ += n))
          case Modified(pod) => pod.getMetadata.flatMap(_.getName).flatMap(n => podNames.update(_ += n))
          case Deleted(pod)  => pod.getMetadata.flatMap(_.getName).flatMap(n => podNames.update(_ -= n) *> dataStore.k8sRef.podMetrics.rm(fcid, n))
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
      .runForeach(m => dataStore.k8sRef.podMetrics.put(m.get).when(m.isDefined))

    for {
      podNames <- Ref.make(mutable.HashSet.empty[String])
      _        <- watchPodNames(podNames).fork
      pollProc <- loopTrigger(flinkConf.tracking.k8sPodMetricsPolling, pollingMetricsApi(podNames))(using flinkConf.tracking.logTrackersFailedInfo)
    } yield pollProc
  }

}
