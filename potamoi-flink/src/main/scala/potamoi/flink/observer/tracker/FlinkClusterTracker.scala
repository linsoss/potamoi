package potamoi.flink.observer.tracker

import akka.actor.typed.{ActorRef, Behavior, PostStop}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import potamoi.akka.ShardingProxy
import potamoi.akka.behaviors.*
import potamoi.akka.zios.*
import potamoi.flink.{flinkRest, FlinkConf, FlinkRestEndpointRetriever, FlinkRestEndpointType}
import potamoi.flink.model.*
import potamoi.flink.FlinkConfigExtension.{InjectedDeploySourceConf, InjectedExecModeKey}
import potamoi.flink.storage.FlinkDataStorage
import potamoi.flink.FlinkErr.ClusterNotFound
import potamoi.kubernetes.K8sErr.RequestK8sApiErr
import potamoi.logger.LogConf
import potamoi.syntax.toPrettyStr
import potamoi.times.given_Conversion_ScalaDuration_ZIODuration
import potamoi.{KryoSerializable, NodeRoles}
import potamoi.options.unsafeGet
import zio.*
import zio.stream.ZStream
import zio.Schedule.{recurWhile, spaced}
import zio.ZIO.{logErrorCause, logInfo}
import zio.ZIOAspect.annotated

import scala.collection.immutable.Set
import scala.collection.mutable
import scala.util.{Failure, Success}
import scala.util.hashing.MurmurHash3

/**
 * Flink cluster snapshot tracker.
 */
object FlinkClusterTracker extends ShardingProxy[Fcid, FlinkClusterTrackerActor.Cmd] {

  private var logConfInstance: Option[LogConf]                                   = None
  private var flinkConfInstance: Option[FlinkConf]                               = None
  private var flinkDataStoreInstance: Option[FlinkDataStorage]                   = None
  private var flinkEndpointRetrieverInstance: Option[FlinkRestEndpointRetriever] = None

  private[tracker] def unsafeLogConf: Option[LogConf]                                   = logConfInstance
  private[tracker] def unsafeFlinkConf: Option[FlinkConf]                               = flinkConfInstance
  private[tracker] def unsafeFlinkDataStore: Option[FlinkDataStorage]                   = flinkDataStoreInstance
  private[tracker] def unsafeFlinkEndpointRetriever: Option[FlinkRestEndpointRetriever] = flinkEndpointRetrieverInstance

  /**
   * Sharding actor manager behavior.
   */
  // noinspection DuplicatedCode
  def apply(logConf: LogConf, flinkConf: FlinkConf, snapStore: FlinkDataStorage, eptRetriever: FlinkRestEndpointRetriever): Behavior[Req] = {
    logConfInstance = Some(logConf)
    flinkConfInstance = Some(flinkConf)
    flinkDataStoreInstance = Some(snapStore)
    flinkEndpointRetrieverInstance = Some(eptRetriever)
    behavior(
      entityKey = EntityTypeKey[FlinkClusterTrackerActor.Cmd]("flink-cluster-tracker"),
      marshallKey = marshallFcid,
      unmarshallKey = unmarshallFcid,
      createBehavior = fcid => FlinkClusterTrackerActor(fcid),
      stopMessage = Some(FlinkClusterTrackerActor.Terminate),
      bindRole = Some(NodeRoles.flinkService)
    )
  }

}

/**
 * Flink cluster snapshot tracker actor.
 */
object FlinkClusterTrackerActor {

  sealed trait Cmd                                     extends KryoSerializable
  final case class Start(reply: ActorRef[Ack.type])    extends Cmd
  final case class Stop(reply: ActorRef[Ack.type])     extends Cmd
  case object Terminate                                extends Cmd
  final case class IsStarted(reply: ActorRef[Boolean]) extends Cmd
  private case class ShouldAutoStart(rs: Boolean)      extends Cmd

  def apply(fcid: Fcid): Behavior[Cmd] = Behaviors.setup { ctx =>
    new FlinkClusterTrackerActor(fcid)(using ctx).active.onFailure[Exception](defaultTrackerFailoverStrategy)
  }
}

import potamoi.flink.observer.tracker.FlinkClusterTrackerActor.*

class FlinkClusterTrackerActor(fcid: Fcid)(using ctx: ActorContext[Cmd]) {

  private given LogConf    = FlinkClusterTracker.unsafeLogConf.unsafeGet
  private val flinkConf    = FlinkClusterTracker.unsafeFlinkConf.unsafeGet
  private val dataStore    = FlinkClusterTracker.unsafeFlinkDataStore.unsafeGet
  private val eptRetriever = FlinkClusterTracker.unsafeFlinkEndpointRetriever.unsafeGet

  private given FlinkRestEndpointType  = flinkConf.restEndpointTypeInternal
  private given logFailReason: Boolean = flinkConf.tracking.logTrackersFailedInfo

  // actor state
  private var isStarted: Boolean                          = false
  private var workProc: Option[CancelableFuture[Unit]]    = None
  private val eptCache: Ref[Option[FlinkRestSvcEndpoint]] = Ref.make(None).runPureSync

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
    .receiveMessage[Cmd] {

      case Start(reply) =>
        if (!isStarted) {
          workProc.map(_.cancel())
          workProc = Some(launchTrackers.runAsync)
          isStarted = true
          ctx.log.info(s"Flink cluster tracker started: ${fcid.show}")
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
      ctx.log.info(s"Flink cluster tracker stopped: ${fcid.show}")
      workProc.map(_.cancel())
      Behaviors.same
    }

  /**
   * Start the track task and all api-polling-based tasks will be blocking until
   * a flink rest k8s endpoint is found for availability.
   */
  private def launchTrackers = {
    for {
      // blocking until the rest service is available in kubernetes.
      _        <- logInfo("Retrieving flink rest endpoint...")
      endpoint <- eptRetriever
                    .retrieve(fcid)
                    .catchAll(_ => ZIO.succeed(None))
                    .repeat(recurWhile[Option[FlinkRestSvcEndpoint]](_.isEmpty) && spaced(1.seconds))
                    .map(_._1.get)
      _        <- logInfo(s"Found flink rest endpoint: ${endpoint.show}")
      _        <- dataStore.restEndpoint.put(fcid, endpoint)
      _        <- eptCache.set(Some(endpoint))

      // blocking until the rest api can be connected.
      _ <- logInfo(s"Checking availability of flink rest endpoint: ${endpoint.show} ...")
      _ <- flinkRest(endpoint.chooseUrl).isAvailable
             .repeat(recurWhile[Boolean](!_) && spaced(500.millis))
             .unit
      _ <- logInfo("Flink rest endpoint is available, let's start all cluster tracking fibers.")

      // launch tracking fibers.
      _ <- pollClusterOverview.fork
      _ <- pollTmDetail.fork
      _ <- pollJmMetrics.fork
      _ <- pollTmMetrics.fork
      _ <- pollJobOverview.fork
      _ <- pollJobMetrics.fork

      // flink svc endpoint cache sync fiber.
      _ <- syncEptValueCache.fork
      _ <- ZIO.never
    } yield ()
  } @@ annotated(fcid.toAnno*)

  /**
   * Sync FlinkSvcEndpoint value from storage.
   */
  private def syncEptValueCache: UIO[Unit] = loopTrigger(
    flinkConf.tracking.eptCacheSyncInterval,
    dataStore.restEndpoint
      .get(fcid)
      .flatMap(eptCache.set)
      .mapError(err => Exception(s"Fail to sync FlinkSvcEndpoint cache inner tracker: $err", err))
  )

  /**
   * Poll flink cluster overview api.
   */
  private def pollClusterOverview: UIO[Unit] = {

    def polling(mur: Ref[Int]) = for {
      restUrl            <- eptCache.get.someOrFail(ClusterNotFound(fcid))
      clusterOvFiber     <- flinkRest(restUrl.chooseUrl).getClusterOverview.fork
      clusterConfigFiber <- flinkRest(restUrl.chooseUrl).getJobmanagerConfig.fork
      clusterOv          <- clusterOvFiber.join
      clusterConfig      <- clusterConfigFiber.join

      isFromPotamoi = clusterConfig.exists(_ == InjectedDeploySourceConf)
      execType      = clusterConfig
                        .get(InjectedExecModeKey)
                        .flatMap(e => FlinkTargetTypes.effectValues.find(_.toString == e))
                        .getOrElse(
                          FlinkTargetTypes.ofRawValue(clusterConfig.getOrElse("execution.target", "unknown")) match
                            case FlinkTargetType.Embedded => FlinkTargetType.K8sApplication
                            case e                        => e
                        )

      preMur <- mur.get
      curMur  = MurmurHash3.productHash(clusterOv -> execType.toString)

      _ <- dataStore.cluster.overview
             .put(clusterOv.toFlinkClusterOverview(fcid, execType, isFromPotamoi))
             .zip(mur.set(curMur))
             .when(preMur != curMur)
    } yield ()

    Ref.make(0).flatMap { mur => loopTrigger(flinkConf.tracking.clusterOvPolling, polling(mur)) }
  }

  /**
   * Poll flink task-manager detail api.
   */
  private def pollTmDetail: UIO[Unit] = {

    def polling(tmMur: Ref[Int], tmIds: Ref[Set[String]]) = for {
      restUrl   <- eptCache.get.someOrFail(ClusterNotFound(fcid))
      tmDetails <-
        ZStream
          .fromIterableZIO(flinkRest(restUrl.chooseUrl).listTaskManagerIds)
          .mapZIOParUnordered(flinkConf.tracking.pollParallelism)(flinkRest(restUrl.chooseUrl).getTaskManagerDetail(_).map(_.toTmDetail(fcid)))
          .runFold(List.empty[FlinkTmDetail])(_ :+ _)

      preMur      <- tmMur.get
      preTmIds    <- tmIds.get
      curMur       = MurmurHash3.arrayHash(tmDetails.toArray)
      curTmIds     = tmDetails.map(_.tmId).toSet
      removedTmIds = preTmIds diff curTmIds

      _ <- ZIO.foreachDiscard(removedTmIds.map(Ftid(fcid, _)))(dataStore.cluster.tmDetail.rm)
      _ <- tmIds.set(curTmIds)
      _ <- dataStore.cluster.tmDetail
             .putAll(tmDetails)
             .zip(tmMur.set(curMur))
             .when(preMur != curMur)
    } yield ()

    for {
      tmMur    <- Ref.make(0)
      tmIds    <- dataStore.cluster.tmDetail
                    .listTmId(fcid)
                    .map(_.map(_.tmId).toSet)
                    .flatMap(Ref.make)
                    .catchAll(_ => Ref.make(Set.empty[String]))
      pollProc <- loopTrigger(flinkConf.tracking.tmdDetailPolling, polling(tmMur, tmIds))
    } yield pollProc
  }

  /**
   * Poll flink job-manager metrics api.
   */
  private def pollJmMetrics: UIO[Unit] = {
    val polling = for {
      restUrl   <- eptCache.get.someOrFail(ClusterNotFound(fcid))
      jmMetrics <- flinkRest(restUrl.chooseUrl).getJmMetrics(FlinkJmMetrics.metricsRawKeys).map(FlinkJmMetrics.fromRaw(fcid, _))
      _         <- dataStore.cluster.jmMetrics.put(jmMetrics)
    } yield ()
    loopTrigger(flinkConf.tracking.jmMetricsPolling, polling)
  }

  /**
   * Polling flink task-manager metrics api.
   */
  private def pollTmMetrics: UIO[Unit] = {
    def polling(tmIds: Ref[Set[String]]) = for {
      restUrl  <- eptCache.get.someOrFail(ClusterNotFound(fcid))
      curTmIds <- flinkRest(restUrl.chooseUrl).listTaskManagerIds.map(_.toSet)
      preTmIds <- tmIds.get

      removedTmIds = preTmIds diff curTmIds
      _           <- ZIO.foreachDiscard(removedTmIds.map(Ftid(fcid, _)))(dataStore.cluster.tmMetrics.rm)
      _           <- tmIds.set(curTmIds)

      _ <- ZStream
             .fromIterable(curTmIds)
             .mapZIOParUnordered(flinkConf.tracking.pollParallelism) { tmId =>
               flinkRest(restUrl.chooseUrl)
                 .getTmMetrics(tmId, FlinkTmMetrics.metricsRawKeys)
                 .map(FlinkTmMetrics.fromRaw(Ftid(fcid, tmId), _)) @@ annotated("tmId" -> tmId)
             }
             .runForeach(dataStore.cluster.tmMetrics.put(_))
    } yield ()

    for {
      tmIds    <- dataStore.cluster.tmMetrics
                    .listTmId(fcid)
                    .map(_.map(_.tmId).toSet)
                    .flatMap(Ref.make)
                    .catchAll(_ => Ref.make(Set.empty[String]))
      pollProc <- loopTrigger(flinkConf.tracking.tmMetricsPolling, polling(tmIds))
    } yield pollProc
  }

  /**
   * Poll flink job overview api.
   */
  private def pollJobOverview: UIO[Unit] = {

    def polling(ovMur: Ref[Int], jobIds: Ref[Set[String]]) = for {
      restUrl <- eptCache.get.someOrFail(ClusterNotFound(fcid))
      jobOvs  <- flinkRest(restUrl.chooseUrl).listJobOverviewInfo.map(_.toSet)

      preOvMur     <- ovMur.get
      preJobIds    <- jobIds.get
      curOvMur      = MurmurHash3.setHash(jobOvs)
      curJobIds     = jobOvs.map(_.jid)
      deletedJobIds = preJobIds diff curJobIds

      _ <- ZIO.foreachDiscard(deletedJobIds.map(Fjid(fcid, _)))(dataStore.job.overview.rm)
      _ <- jobIds.set(curJobIds)
      _ <- dataStore.job.overview
             .putAll(jobOvs.map(_.toFlinkJobOverview(fcid)).toList)
             .zip(ovMur.set(curOvMur))
             .when(preOvMur != curOvMur)
    } yield ()

    for {
      ovMur    <- Ref.make(0)
      jobIds   <- dataStore.job.overview
                    .listJobId(fcid)
                    .map(_.map(_.jobId).toSet)
                    .flatMap(Ref.make)
                    .catchAll(_ => Ref.make(Set.empty[String]))
      pollProc <- loopTrigger(flinkConf.tracking.jobOvPolling, polling(ovMur, jobIds))
    } yield pollProc
  }

  /**
   * Poll flink job metrics api.
   */
  private def pollJobMetrics: UIO[Unit] = {

    def polling(jobIds: Ref[Set[String]]) = for {
      restUrl   <- eptCache.get.someOrFail(ClusterNotFound(fcid))
      curJobIds <- flinkRest(restUrl.chooseUrl).listJobsStatusInfo.map(_.map(_.id).toSet)
      preJobIds <- jobIds.get

      removedJobIds = preJobIds diff curJobIds
      _            <- ZIO.foreachDiscard(removedJobIds.map(Fjid(fcid, _)))(dataStore.job.metrics.rm)
      _            <- jobIds.set(curJobIds)

      _ <- ZStream
             .fromIterable(curJobIds)
             .mapZIOParUnordered(flinkConf.tracking.pollParallelism) { jobId =>
               flinkRest(restUrl.chooseUrl)
                 .getJobMetrics(jobId, FlinkJobMetrics.metricsRawKeys)
                 .map(FlinkJobMetrics.fromRaw(Fjid(fcid, jobId), _)) @@ annotated("jobId" -> jobId)
             }
             .runForeach(dataStore.job.metrics.put(_))
    } yield ()

    for {
      jobIds   <- dataStore.job.metrics
                    .listJobId(fcid)
                    .map(_.map(_.jobId).toSet)
                    .flatMap(Ref.make)
                    .catchAll(_ => Ref.make(Set.empty[String]))
      pollProc <- loopTrigger(flinkConf.tracking.jobMetricsPolling, polling(jobIds))
    } yield pollProc
  }
}
