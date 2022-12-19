package potamoi.flink.tracker

import com.devsisters.shardcake.*
import potamoi.flink.model.{Fcid, Fjid, FlinkExecModes, FlinkJmMetrics, FlinkJobMetrics, FlinkRestSvcEndpoint, FlinkTmDetail, FlinkTmMetrics, Ftid}
import potamoi.flink.observer.RestEndpointsQuery
import potamoi.flink.{flinkRest, FlinkConf, FlinkRestEndpointType}
import potamoi.flink.FlinkConfigExtension.{InjectedDeploySourceConf, InjectedExecModeKey}
import potamoi.flink.storage.FlinkSnapshotStorage
import potamoi.flink.FlinkErr.ClusterNotFound
import potamoi.kubernetes.K8sErr.RequestK8sApiErr
import potamoi.times.given_Conversion_ScalaDuration_ZioDuration
import potamoi.syntax.toPrettyStr
import zio.*
import zio.stream.ZStream
import zio.ZIO.logInfo
import zio.Schedule.{recurWhile, spaced}
import zio.ZIOAspect.annotated

import scala.collection.mutable
import scala.util.hashing.MurmurHash3

/**
 * Flink cluster snapshot tracker.
 */
object FlinkClusterTracker {
  sealed trait Cmd
  case object Start                               extends Cmd
  case object Stop                                extends Cmd
  case class IsStarted(replier: Replier[Boolean]) extends Cmd

  object Entity extends EntityType[Cmd]("flinkClusterTracker")
}

class FlinkClusterTracker(flinkConf: FlinkConf, snapStg: FlinkSnapshotStorage, eptRetriever: FlinkRestEndpointRetriever) {
  import FlinkClusterTracker.*

  private given FlinkRestEndpointType = flinkConf.restEndpointTypeInternal
  private type TrackTaskFiber = Fiber.Runtime[Nothing, Unit]
  private type LaunchFiber    = Fiber.Runtime[Throwable, Unit]

  /**
   * Sharding entity behavior.
   */
  def behavior(entityId: String, messages: Dequeue[Cmd]): RIO[Sharding, Nothing] =
    for {
      isStarted      <- Ref.make(false)
      pollFiberRef   <- Ref.make(mutable.Set.empty[TrackTaskFiber])
      launchFiberRef <- Ref.make[Option[LaunchFiber]](None)
      fcid = unmarshallFcid(entityId)
      effect <- messages.take.flatMap(handleMessage(fcid, _, isStarted, launchFiberRef, pollFiberRef)).forever
    } yield effect

  private def handleMessage(
      fcid: Fcid,
      message: Cmd,
      isStarted: Ref[Boolean],
      launchFiberRef: Ref[Option[LaunchFiber]],
      trackTaskFiberRef: Ref[mutable.Set[TrackTaskFiber]]): RIO[Sharding, Unit] = {
    message match {
      case Start =>
        isStarted.get.flatMap {
          case true => ZIO.unit
          case false =>
            for {
              _           <- logInfo(s"Flink cluster tracker started: ${fcid.show}")
              _           <- clearTrackTaskFibers(trackTaskFiberRef)
              launchFiber <- launchTrackers(fcid, trackTaskFiberRef).forkDaemon
              _           <- launchFiberRef.update(_ => Some(launchFiber))
              _           <- isStarted.set(true)
            } yield ()
        }
      case Stop =>
        logInfo(s"Flink cluster tracker stopped: ${fcid.show}") *>
        clearTrackTaskFibers(trackTaskFiberRef) *>
        isStarted.set(false)
      case IsStarted(replier) => isStarted.get.flatMap(replier.reply)
    }
  } @@ ZIOAspect.annotated(fcid.toAnno*)

  /**
   * Start the track task and all api-polling-based tasks will be blocking until
   * a flink rest k8s endpoint is founded for availability.
   */
  private def launchTrackers(fcid: Fcid, trackTaskFibers: Ref[mutable.Set[TrackTaskFiber]]): IO[Throwable, Unit] =
    for {
      _ <- logInfo(s"Retrieving flink rest endpoint...")
      endpoint <- eptRetriever
        .retrieve(fcid)
        .catchAll(_ => ZIO.succeed(None)) // ignore all error
        .repeat(recurWhile[Option[FlinkRestSvcEndpoint]](_.isEmpty) && spaced(1.seconds))
        .map(_._1.get)
      _              <- logInfo(s"Found flink rest endpoint: ${endpoint.toPrettyStr}")
      _              <- snapStg.restEndpoint.put(fcid, endpoint)
      clusterOvFiber <- pollClusterOverview(fcid).forkDaemon
      tmDetailFiber  <- pollTmDetail(fcid).fork
      jmMetricFiber  <- pollJmMetrics(fcid).fork
      tmMetricFiber  <- pollTmMetrics(fcid).fork
      jobOvFiber     <- pollJobOverview(fcid).fork
      jobMetricFiber <- pollJobMetrics(fcid).fork
      _              <- trackTaskFibers.update(_ ++= Set(clusterOvFiber, tmDetailFiber, jmMetricFiber, tmMetricFiber, jobOvFiber, jobMetricFiber))
    } yield ()

  // noinspection DuplicatedCode
  private def clearTrackTaskFibers(pollFibers: Ref[mutable.Set[TrackTaskFiber]]) =
    for {
      fibers <- pollFibers.get
      _      <- fibers.map(_.interrupt).reduce(_ *> _)
      _      <- pollFibers.set(mutable.Set.empty)
    } yield ()

  /**
   * Poll flink cluster overview api.
   */
  private def pollClusterOverview(fcid: Fcid): UIO[Unit] = {
    def polling(mur: Ref[Int]) = for {
      restUrl            <- snapStg.restEndpoint.get(fcid).someOrFail(ClusterNotFound(fcid))
      clusterOvFiber     <- flinkRest(restUrl.chooseUrl).getClusterOverview.fork
      clusterConfigFiber <- flinkRest(restUrl.chooseUrl).getJobmanagerConfig.fork
      clusterOv          <- clusterOvFiber.join
      clusterConfig      <- clusterConfigFiber.join

      isFromPotamoi = clusterConfig.exists(_ == InjectedDeploySourceConf)
      execMode = clusterConfig
        .get(InjectedExecModeKey)
        .flatMap(FlinkExecModes.valueOfOption)
        .getOrElse(FlinkExecModes.ofRawConfValue(clusterConfig.get("execution.target")))

      preMur <- mur.get
      curMur = MurmurHash3.productHash(clusterOv -> execMode.value)

      _ <- snapStg.cluster.overview
        .put(clusterOv.toFlinkClusterOverview(fcid, execMode, isFromPotamoi))
        .zip(mur.set(curMur))
        .when(preMur != curMur)
    } yield ()
    Ref.make(0).flatMap { mur => loopTrigger(flinkConf.tracking.clusterOvPolling, polling(mur)) }
  }

  /**
   * Poll flink task-manager detail api.
   */
  private def pollTmDetail(fcid: Fcid): UIO[Unit] = {

    def polling(tmMur: Ref[Int], tmIds: Ref[Set[String]]) = for {
      restUrl <- snapStg.restEndpoint.get(fcid).someOrFail(ClusterNotFound(fcid))
      tmDetails <- ZStream
        .fromIterableZIO(flinkRest(restUrl.chooseUrl).listTaskManagerIds)
        .mapZIOParUnordered(flinkConf.tracking.pollParallelism)(flinkRest(restUrl.chooseUrl).getTaskManagerDetail)
        .runFold(List.empty[FlinkTmDetail])(_ :+ _)

      preMur   <- tmMur.get
      preTmIds <- tmIds.get
      curMur       = MurmurHash3.arrayHash(tmDetails.toArray)
      curTmIds     = tmDetails.map(_.tmId).toSet
      removedTmIds = preTmIds diff curTmIds

      _ <- removedTmIds.map(Ftid(fcid, _)).map(snapStg.cluster.tmDetail.rm(_)).reduce(_ *> _)
      _ <- tmIds.set(curTmIds)
      _ <- snapStg.cluster.tmDetail
        .putAll(tmDetails)
        .zip(tmMur.set(curMur))
        .when(preMur != curMur)
    } yield ()

    for {
      tmMur <- Ref.make(0)
      tmIds <- snapStg.cluster.tmDetail
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
  private def pollJmMetrics(fcid: Fcid): UIO[Unit] = {
    val polling = for {
      restUrl   <- snapStg.restEndpoint.get(fcid).someOrFail(ClusterNotFound(fcid))
      jmMetrics <- flinkRest(restUrl.chooseUrl).getJmMetrics(FlinkJmMetrics.metricsRawKeys).map(FlinkJmMetrics.fromRaw(fcid, _))
      _         <- snapStg.cluster.jmMetrics.put(jmMetrics)
    } yield ()
    loopTrigger(flinkConf.tracking.jmMetricsPolling, polling)
  }

  /**
   * Polling flink task-manager metrics api.
   */
  private def pollTmMetrics(fcid: Fcid): UIO[Unit] = {
    def polling(tmIds: Ref[Set[String]]) = for {
      restUrl  <- snapStg.restEndpoint.get(fcid).someOrFail(ClusterNotFound(fcid))
      curTmIds <- flinkRest(restUrl.chooseUrl).listTaskManagerIds.map(_.toSet)
      preTmIds <- tmIds.get

      removedTmIds = preTmIds diff curTmIds
      _ <- removedTmIds.map(Ftid(fcid, _)).map(snapStg.cluster.tmMetrics.rm(_)).reduce(_ *> _)
      _ <- tmIds.set(curTmIds)

      _ <- ZStream
        .fromIterable(curTmIds)
        .mapZIOParUnordered(flinkConf.tracking.pollParallelism) { tmId =>
          flinkRest(restUrl.chooseUrl)
            .getTmMetrics(tmId, FlinkTmMetrics.metricsRawKeys)
            .map(FlinkTmMetrics.fromRaw(Ftid(fcid, tmId), _)) @@ annotated("tmId" -> tmId)
        }
        .runForeach(snapStg.cluster.tmMetrics.put(_))
    } yield ()

    for {
      tmIds <- snapStg.cluster.tmMetrics
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
  private def pollJobOverview(fcid: Fcid): UIO[Unit] = {

    def polling(ovMur: Ref[Int], jobIds: Ref[Set[String]]) = for {
      restUrl <- snapStg.restEndpoint.get(fcid).someOrFail(ClusterNotFound(fcid))
      jobOvs  <- flinkRest(restUrl.chooseUrl).listJobOverviewInfo.map(_.toSet)

      preOvMur  <- ovMur.get
      preJobIds <- jobIds.get
      curOvMur      = MurmurHash3.setHash(jobOvs)
      curJobIds     = jobOvs.map(_.jid)
      deletedJobIds = preJobIds diff curJobIds

      _ <- deletedJobIds.map(Fjid(fcid, _)).map(snapStg.job.overview.rm(_)).reduce(_ *> _)
      _ <- jobIds.set(curJobIds)
      _ <- snapStg.job.overview
        .putAll(jobOvs.map(_.toFlinkJobOverview(fcid)).toList)
        .zip(ovMur.set(curOvMur))
        .when(preOvMur != curOvMur)
    } yield ()

    for {
      ovMur <- Ref.make(0)
      jobIds <- snapStg.job.overview
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
  private def pollJobMetrics(fcid: Fcid): UIO[Unit] = {

    def polling(jobIds: Ref[Set[String]]) = for {
      restUrl   <- snapStg.restEndpoint.get(fcid).someOrFail(ClusterNotFound(fcid))
      curJobIds <- flinkRest(restUrl.chooseUrl).listJobsStatusInfo.map(_.map(_.id).toSet)
      preJobIds <- jobIds.get

      removedJobIds = preJobIds diff curJobIds
      _ <- removedJobIds.map(Fjid(fcid, _)).map(snapStg.job.metrics.rm(_)).reduce(_ *> _)
      _ <- jobIds.set(curJobIds)

      _ <- ZStream
        .fromIterable(curJobIds)
        .mapZIOParUnordered(flinkConf.tracking.pollParallelism) { jobId =>
          flinkRest(restUrl.chooseUrl)
            .getJobMetrics(jobId, FlinkJobMetrics.metricsRawKeys)
            .map(FlinkJobMetrics.fromRaw(Fjid(fcid, jobId), _)) @@ annotated("jobId" -> jobId)
        }
        .runForeach(snapStg.job.metrics.put(_))
    } yield ()

    for {
      jobIds <- snapStg.job.metrics
        .listJobId(fcid)
        .map(_.map(_.jobId).toSet)
        .flatMap(Ref.make)
        .catchAll(_ => Ref.make(Set.empty[String]))
      pollProc <- loopTrigger(flinkConf.tracking.jobMetricsPolling, polling(jobIds))
    } yield pollProc
  }

}
