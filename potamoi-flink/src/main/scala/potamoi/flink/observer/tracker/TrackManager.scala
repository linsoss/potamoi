package potamoi.flink.observer.tracker

import akka.actor.typed.ActorRef
import potamoi.flink.{FlinkConf, FlinkDataStoreErr, FlinkErr, FlinkRestEndpointRetriever}
import potamoi.flink.model.Fcid
import potamoi.flink.storage.{FlinkDataStorage, TrackedFcidStorage}
import potamoi.flink.FlinkErr.AkkaErr
import potamoi.kubernetes.K8sOperator
import potamoi.EarlyLoad
import potamoi.akka.AkkaMatrix
import potamoi.flink.observer.tracker.FlinkClusterTracker.ops
import potamoi.flink.observer.tracker.FlinkK8sRefTracker.ops
import potamoi.flink.observer.tracker.TrackerManager.TrackClusterErr
import potamoi.logger.LogConf
import zio.{IO, RIO, Scope, Task, UIO, URIO, ZIO, ZIOAspect}
import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}
import zio.stream.{Stream, UStream, ZStream}
import potamoi.zios.union

/**
 * Flink cluster trackers manager.
 */
trait TrackManager {

  /**
   * Tracking flink cluster.
   */
  def track(fcid: Fcid): IO[TrackClusterErr, Unit]

  /**
   * UnTracking flink cluster.
   */
  def untrack(fcid: Fcid): IO[TrackClusterErr, Unit]

  /**
   * Whether the tracked fcid exists.
   */
  def isBeTracked(fcid: Fcid): IO[FlinkDataStoreErr, Boolean]

  /**
   * Listing tracked cluster id.
   */
  def listTrackedClusters: Stream[FlinkDataStoreErr, Fcid]

  /**
   * Get trackers status.
   */
  def getTrackersStatus(fcid: Fcid): UIO[TrackersStatus]

  /**
   * list all trackers status.
   */
  def listTrackersStatus(parallelism: Int = 12): Stream[FlinkDataStoreErr, TrackersStatus]

}

object TrackerManager {

  type TrackClusterErr = (FlinkDataStoreErr | AkkaErr) with FlinkErr

  def make(
      logConf: LogConf,
      flinkConf: FlinkConf,
      actorCradle: AkkaMatrix,
      snapStorage: FlinkDataStorage,
      eptRetriever: FlinkRestEndpointRetriever,
      k8sOperator: K8sOperator): ZIO[Any, Throwable, TrackManager] =
    for {
      clusterTrackerProxy <- actorCradle.spawn("flink-cluster-trackers", FlinkClusterTracker(logConf, flinkConf, snapStorage, eptRetriever))
      k8sRefTrackerProxy  <- actorCradle.spawn("flink-k8s-trackers", FlinkK8sRefTracker(logConf, flinkConf, snapStorage, k8sOperator))
      given AkkaMatrix     = actorCradle
    } yield new TrackManagerImpl(snapStorage, clusterTrackerProxy, k8sRefTrackerProxy)
}

/**
 * Default implementation.
 */
class TrackManagerImpl(
    snapStorage: FlinkDataStorage,
    clusterTrackers: ActorRef[FlinkClusterTracker.Req],
    k8sRefTrackers: ActorRef[FlinkK8sRefTracker.Req]
  )(using AkkaMatrix)
    extends TrackManager {

  /**
   * Tracking flink cluster.
   */
  override def track(fcid: Fcid): IO[TrackClusterErr, Unit] =
    union[TrackClusterErr, Unit] {
      for
        _ <- snapStorage.trackedList.put(fcid)
        _ <- clusterTrackers(fcid).askZIO(FlinkClusterTrackerActor.Start.apply).mapError(AkkaErr.apply)
        _ <- k8sRefTrackers(fcid).askZIO(FlinkK8sRefTrackerActor.Start.apply).mapError(AkkaErr.apply)
      yield ()
    } @@ ZIOAspect.annotated(fcid.toAnno*)

  /**
   * UnTracking flink cluster.
   */
  override def untrack(fcid: Fcid): IO[TrackClusterErr, Unit] =
    union[TrackClusterErr, Unit] {
      for
        _ <- snapStorage.trackedList.rm(fcid)
        _ <- clusterTrackers(fcid).askZIO(FlinkClusterTrackerActor.Stop.apply).mapError(AkkaErr.apply)
        _ <- k8sRefTrackers(fcid).askZIO(FlinkK8sRefTrackerActor.Stop.apply).mapError(AkkaErr.apply)
        // remove all snapshot data belongs to fcid
        _ <- snapStorage.rmSnapData(fcid)
      yield ()
    } @@ ZIOAspect.annotated(fcid.toAnno*)

  /**
   * Whether the tracked fcid exists.
   */
  override def isBeTracked(fcid: Fcid): IO[FlinkDataStoreErr, Boolean] = snapStorage.trackedList.exists(fcid)

  /**
   * Listing tracked cluster id.
   */
  override def listTrackedClusters: Stream[FlinkDataStoreErr, Fcid] = snapStorage.trackedList.list

  /**
   * Get trackers status.
   */
  override def getTrackersStatus(fcid: Fcid): IO[Nothing, TrackersStatus] = {
    {
      askStatus(clusterTrackers(fcid).askZIO(FlinkClusterTrackerActor.IsStarted.apply)) <&>
      askStatus(k8sRefTrackers(fcid).askZIO(FlinkK8sRefTrackerActor.IsStarted.apply))
    } map { case (clusterTrk, k8sRefTrk) => TrackersStatus(fcid, clusterTrk, k8sRefTrk) }
  }

  private def askStatus(io: Task[Boolean]) =
    io.map(if _ then TrackerState.Running else TrackerState.Idle)
      .catchAllCause(cause => ZIO.logErrorCause(cause) *> ZIO.succeed(TrackerState.Unknown))

  /**
   * list all trackers status.
   */
  override def listTrackersStatus(parallelism: Int): Stream[FlinkDataStoreErr, TrackersStatus] =
    snapStorage.trackedList.list.mapZIOParUnordered(parallelism)(getTrackersStatus)

}
