package potamoi.flink.observer.tracker

import com.devsisters.shardcake.*
import potamoi.flink.{flinkRest, FlinkConf, FlinkRestEndpointRetriever, FlinkRestEndpointType}
import potamoi.flink.model.*
import potamoi.flink.FlinkConfigExtension.{InjectedDeploySourceConf, InjectedExecModeKey}
import potamoi.flink.storage.FlinkDataStorage
import potamoi.flink.FlinkErr.ClusterNotFound
import potamoi.kubernetes.K8sErr.RequestK8sApiErr
import potamoi.syntax.toPrettyStr
import potamoi.times.given_Conversion_ScalaDuration_ZioDuration
import zio.*
import zio.stream.ZStream
import zio.Schedule.{recurWhile, spaced}
import zio.ZIO.{logErrorCause, logInfo}
import zio.ZIOAspect.annotated

import scala.collection.mutable
import scala.util.hashing.MurmurHash3

/**
 * Flink cluster snapshot tracker.
 */
object FlinkClusterTracker {

  object Entity extends EntityType[Cmd]("flinkClusterTracker")

  sealed trait Cmd
  case class Start(replier: Replier[Ack.type])    extends Cmd
  case class Stop(replier: Replier[Ack.type])     extends Cmd
  case object Terminate                           extends Cmd
  case class IsStarted(replier: Replier[Boolean]) extends Cmd

  // noinspection DuplicatedCode
  private case class TrackerState(
      isStarted: Ref[Boolean],
      launchFiberRef: Ref[Option[Fiber.Runtime[_, _]]],
      trackTaskFibersRef: Ref[Set[Fiber.Runtime[_, _]]],
      flinkEptCache: Ref[Option[FlinkRestSvcEndpoint]]):
    def reset: UIO[Unit] =
      for {
        _ <- launchFiberRef.getAndSet(None).flatMap {
               case None        => ZIO.unit
               case Some(fiber) => fiber.interrupt
             }
        _ <- trackTaskFibersRef.getAndSet(Set.empty).flatMap { fibers =>
               ZIO.foreachPar(fibers)(_.interrupt)
             }
        _ <- flinkEptCache.set(None)
        _ <- isStarted.set(false)
      } yield ()

  private object TrackerState:
    def make: UIO[TrackerState] =
      for {
        isStarted          <- Ref.make[Boolean](false)
        launchFiberRef     <- Ref.make[Option[Fiber.Runtime[_, _]]](None)
        trackTaskFibersRef <- Ref.make[Set[Fiber.Runtime[_, _]]](Set.empty)
        flinkEptCache      <- Ref.make[Option[FlinkRestSvcEndpoint]](None)
      } yield TrackerState(isStarted, launchFiberRef, trackTaskFibersRef, flinkEptCache)
}

class FlinkClusterTracker(flinkConf: FlinkConf, snapStore: FlinkDataStorage, eptRetriever: FlinkRestEndpointRetriever) {

  import FlinkClusterTracker.*
  private given FlinkRestEndpointType  = flinkConf.restEndpointTypeInternal
  private given logFailReason: Boolean = flinkConf.tracking.logTrackersFailedInfo

  /**
   * Sharding entity behavior.
   */
  def behavior(entityId: String, messages: Dequeue[Cmd]): RIO[Sharding with Scope, Nothing] =
    for {
      fcid   <- ZIO.attempt(unmarshallFcid(entityId)).tapErrorCause(cause => ZIO.logErrorCause(s"Fail to unmarshall Fcid: $entityId", cause))
      state  <- TrackerState.make
      // start tracking automatically if needs.
      _      <- snapStore.trackedList
                  .exists(fcid)
                  .catchAll(_ => ZIO.succeed(false))
                  .flatMap { shouldAutoStart =>
                    if shouldAutoStart then start(fcid)(using state) else ZIO.unit
                  }
                  .forkScoped
      effect <- messages.take.flatMap(handleMessage(fcid, _)(using state)).forever
    } yield effect

  private def handleMessage(fcid: Fcid, message: Cmd)(using state: TrackerState): RIO[Sharding with Scope, Unit] = {
    message match {
      case Start(replier)     => start(fcid) *> replier.reply(Ack)
      case Stop(replier)      => logInfo(s"Flink cluster tracker stopped: ${fcid.show}") *> state.reset *> replier.reply(Ack)
      case Terminate          => logInfo(s"Flink cluster tracker terminated: ${fcid.show}") *> state.reset
      case IsStarted(replier) => state.isStarted.get.flatMap(replier.reply)
    }
  } @@ ZIOAspect.annotated(fcid.toAnno*)

  private def start(fcid: Fcid)(using state: TrackerState): RIO[Scope, Unit] = {
    state.isStarted.get.flatMap {
      case true  => ZIO.unit
      case false =>
        for {
          _           <- logInfo(s"Flink cluster tracker started: ${fcid.show}")
          _           <- state.reset
          launchFiber <- launchTrackers(fcid).forkScoped
          _           <- state.launchFiberRef.update(_ => Some(launchFiber))
          _           <- state.isStarted.set(true)

        } yield ()
    }
  }

  /**
   * Start the track task and all api-polling-based tasks will be blocking until
   * a flink rest k8s endpoint is found for availability.
   */
  private def launchTrackers(fcid: Fcid)(using state: TrackerState): RIO[Scope, Unit] =
    for {
      // blocking until the rest service is available in kubernetes.
      _        <- logInfo("Retrieving flink rest endpoint...")
      endpoint <- eptRetriever
                    .retrieve(fcid)
                    .catchAll(_ => ZIO.succeed(None))
                    .repeat(recurWhile[Option[FlinkRestSvcEndpoint]](_.isEmpty) && spaced(1.seconds))
                    .map(_._1.get)
      _        <- logInfo(s"Found flink rest endpoint: ${endpoint.show}")
      _        <- snapStore.restEndpoint.put(fcid, endpoint)
      _        <- state.flinkEptCache.set(Some(endpoint))

      // blocking until the rest api can be connected.
      _ <- logInfo(s"Checking availability of flink rest endpoint: ${endpoint.show} ...")
      _ <- flinkRest(endpoint.chooseUrl).isAvailable
             .repeat(recurWhile[Boolean](!_) && spaced(500.millis))
             .unit
      _ <- logInfo("Flink rest endpoint is available, let's start all cluster tracking fibers.")

      given Ref[Option[FlinkRestSvcEndpoint]] = state.flinkEptCache

      // launch tracking fibers.
      clusterOvFiber    <- pollClusterOverview(fcid).forkScoped
      tmDetailFiber     <- pollTmDetail(fcid).forkScoped
      jmMetricFiber     <- pollJmMetrics(fcid).forkScoped
      tmMetricFiber     <- pollTmMetrics(fcid).forkScoped
      jobOvFiber        <- pollJobOverview(fcid).forkScoped
      jobMetricFiber    <- pollJobMetrics(fcid).forkScoped
      syncEptValueFiber <- syncEptValueCache(fcid).forkScoped
      _                 <- state.trackTaskFibersRef.set(
                             Set(
                               clusterOvFiber,
                               tmDetailFiber,
                               jmMetricFiber,
                               tmMetricFiber,
                               jobOvFiber,
                               jobMetricFiber,
                               syncEptValueFiber
                             ))
    } yield ()

  /**
   * Sync FlinkSvcEndpoint value from storage.
   */
  private def syncEptValueCache(fcid: Fcid)(using state: TrackerState): UIO[Unit] = loopTrigger(
    flinkConf.tracking.eptCacheSyncInterval,
    snapStore.restEndpoint
      .get(fcid)
      .flatMap(state.flinkEptCache.set)
      .mapError(err => Exception(s"Fail to sync FlinkSvcEndpoint cache inner tracker: $err", err))
  )

  /**
   * Poll flink cluster overview api.
   */
  private def pollClusterOverview(fcid: Fcid)(using eptCache: Ref[Option[FlinkRestSvcEndpoint]]): UIO[Unit] = {

    def polling(mur: Ref[Int]) = for {
      restUrl            <- eptCache.get.someOrFail(ClusterNotFound(fcid))
      clusterOvFiber     <- flinkRest(restUrl.chooseUrl).getClusterOverview.fork
      clusterConfigFiber <- flinkRest(restUrl.chooseUrl).getJobmanagerConfig.fork
      clusterOv          <- clusterOvFiber.join
      clusterConfig      <- clusterConfigFiber.join

      isFromPotamoi = clusterConfig.exists(_ == InjectedDeploySourceConf)
      execType      = clusterConfig
                        .get(InjectedExecModeKey)
                        .flatMap(e => FlinkTargetTypes.values.find(_.toString == e))
                        .getOrElse(
                          FlinkTargetTypes.ofRawValue(clusterConfig.getOrElse("execution.target", "unknown")) match
                            case FlinkTargetType.Embedded => FlinkTargetType.K8sApplication
                            case e                        => e
                        )

      preMur <- mur.get
      curMur  = MurmurHash3.productHash(clusterOv -> execType.toString)

      _ <- snapStore.cluster.overview
             .put(clusterOv.toFlinkClusterOverview(fcid, execType, isFromPotamoi))
             .zip(mur.set(curMur))
             .when(preMur != curMur)
    } yield ()
    Ref.make(0).flatMap { mur => loopTrigger(flinkConf.tracking.clusterOvPolling, polling(mur)) }
  }

  /**
   * Poll flink task-manager detail api.
   */
  private def pollTmDetail(fcid: Fcid)(using eptCache: Ref[Option[FlinkRestSvcEndpoint]]): UIO[Unit] = {

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

      _ <- ZIO.foreachDiscard(removedTmIds.map(Ftid(fcid, _)))(snapStore.cluster.tmDetail.rm)
      _ <- tmIds.set(curTmIds)
      _ <- snapStore.cluster.tmDetail
             .putAll(tmDetails)
             .zip(tmMur.set(curMur))
             .when(preMur != curMur)
    } yield ()

    for {
      tmMur    <- Ref.make(0)
      tmIds    <- snapStore.cluster.tmDetail
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
  private def pollJmMetrics(fcid: Fcid)(using eptCache: Ref[Option[FlinkRestSvcEndpoint]]): UIO[Unit] = {
    val polling = for {
      restUrl   <- eptCache.get.someOrFail(ClusterNotFound(fcid))
      jmMetrics <- flinkRest(restUrl.chooseUrl).getJmMetrics(FlinkJmMetrics.metricsRawKeys).map(FlinkJmMetrics.fromRaw(fcid, _))
      _         <- snapStore.cluster.jmMetrics.put(jmMetrics)
    } yield ()
    loopTrigger(flinkConf.tracking.jmMetricsPolling, polling)
  }

  /**
   * Polling flink task-manager metrics api.
   */
  private def pollTmMetrics(fcid: Fcid)(using eptCache: Ref[Option[FlinkRestSvcEndpoint]]): UIO[Unit] = {
    def polling(tmIds: Ref[Set[String]]) = for {
      restUrl  <- eptCache.get.someOrFail(ClusterNotFound(fcid))
      curTmIds <- flinkRest(restUrl.chooseUrl).listTaskManagerIds.map(_.toSet)
      preTmIds <- tmIds.get

      removedTmIds = preTmIds diff curTmIds
      _           <- ZIO.foreachDiscard(removedTmIds.map(Ftid(fcid, _)))(snapStore.cluster.tmMetrics.rm)
      _           <- tmIds.set(curTmIds)

      _ <- ZStream
             .fromIterable(curTmIds)
             .mapZIOParUnordered(flinkConf.tracking.pollParallelism) { tmId =>
               flinkRest(restUrl.chooseUrl)
                 .getTmMetrics(tmId, FlinkTmMetrics.metricsRawKeys)
                 .map(FlinkTmMetrics.fromRaw(Ftid(fcid, tmId), _)) @@ annotated("tmId" -> tmId)
             }
             .runForeach(snapStore.cluster.tmMetrics.put(_))
    } yield ()

    for {
      tmIds    <- snapStore.cluster.tmMetrics
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
  private def pollJobOverview(fcid: Fcid)(using eptCache: Ref[Option[FlinkRestSvcEndpoint]]): UIO[Unit] = {

    def polling(ovMur: Ref[Int], jobIds: Ref[Set[String]]) = for {
      restUrl <- eptCache.get.someOrFail(ClusterNotFound(fcid))
      jobOvs  <- flinkRest(restUrl.chooseUrl).listJobOverviewInfo.map(_.toSet)

      preOvMur     <- ovMur.get
      preJobIds    <- jobIds.get
      curOvMur      = MurmurHash3.setHash(jobOvs)
      curJobIds     = jobOvs.map(_.jid)
      deletedJobIds = preJobIds diff curJobIds

      _ <- ZIO.foreachDiscard(deletedJobIds.map(Fjid(fcid, _)))(snapStore.job.overview.rm)
      _ <- jobIds.set(curJobIds)
      _ <- snapStore.job.overview
             .putAll(jobOvs.map(_.toFlinkJobOverview(fcid)).toList)
             .zip(ovMur.set(curOvMur))
             .when(preOvMur != curOvMur)
    } yield ()

    for {
      ovMur    <- Ref.make(0)
      jobIds   <- snapStore.job.overview
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
  private def pollJobMetrics(fcid: Fcid)(using eptCache: Ref[Option[FlinkRestSvcEndpoint]]): UIO[Unit] = {

    def polling(jobIds: Ref[Set[String]]) = for {
      restUrl   <- eptCache.get.someOrFail(ClusterNotFound(fcid))
      curJobIds <- flinkRest(restUrl.chooseUrl).listJobsStatusInfo.map(_.map(_.id).toSet)
      preJobIds <- jobIds.get

      removedJobIds = preJobIds diff curJobIds
      _            <- ZIO.foreachDiscard(removedJobIds.map(Fjid(fcid, _)))(snapStore.job.metrics.rm)
      _            <- jobIds.set(curJobIds)

      _ <- ZStream
             .fromIterable(curJobIds)
             .mapZIOParUnordered(flinkConf.tracking.pollParallelism) { jobId =>
               flinkRest(restUrl.chooseUrl)
                 .getJobMetrics(jobId, FlinkJobMetrics.metricsRawKeys)
                 .map(FlinkJobMetrics.fromRaw(Fjid(fcid, jobId), _)) @@ annotated("jobId" -> jobId)
             }
             .runForeach(snapStore.job.metrics.put(_))
    } yield ()

    for {
      jobIds   <- snapStore.job.metrics
                    .listJobId(fcid)
                    .map(_.map(_.jobId).toSet)
                    .flatMap(Ref.make)
                    .catchAll(_ => Ref.make(Set.empty[String]))
      pollProc <- loopTrigger(flinkConf.tracking.jobMetricsPolling, polling(jobIds))
    } yield pollProc
  }

}
