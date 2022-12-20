package potamoi.flink.observer.tracker

import potamoi.flink.{DataStorageErr, FlinkConf, FlinkErr, FlinkRestEndpointRetriever}
import potamoi.flink.model.Fcid
import potamoi.flink.storage.{FlinkSnapshotStorage, TrackedFcidStorage}
import com.devsisters.shardcake.{Messenger, Sharding}
import potamoi.kubernetes.K8sOperator
import potamoi.sharding.ShardRegister
import zio.{IO, RIO, Scope, Task, UIO, URIO, ZIO}
import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}
import zio.stream.{Stream, UStream, ZStream}

import scala.util.Try

/**
 * Flink cluster trackers manager.
 */
trait TrackManager extends ShardRegister {

  /**
   * Tracking flink cluster.
   */
  def track(fcid: Fcid): IO[FlinkErr, Unit]

  /**
   * UnTracking flink cluster.
   */
  def untrack(fcid: Fcid): IO[FlinkErr, Unit]

  /**
   * Whether the tracked fcid exists.
   */
  def isBeTracked(fcid: Fcid): IO[FlinkErr, Boolean]

  /**
   * Listing tracked cluster id.
   */
  def listTrackedClusters: Stream[FlinkErr, Fcid]

  /**
   * Get trackers status.
   */
  def getTrackersStatus(fcid: Fcid): IO[FlinkErr, TrackersStatus]

  /**
   * list all trackers status.
   */
  def listTrackersStatus(parallelism: Int = 12): Stream[FlinkErr, TrackersStatus]

}

private val ClusterTrk = FlinkClusterTracker
private val K8sRefTrk  = FlinkK8sRefTracker

object TrackerManager {
  def instance(
      flinkConf: FlinkConf,
      snapStorage: FlinkSnapshotStorage,
      eptRetriever: FlinkRestEndpointRetriever,
      k8sOperator: K8sOperator): URIO[Sharding, TrackManagerLive] =
    for {
      _ <- ZIO.unit
      clusterTracker = FlinkClusterTracker(flinkConf, snapStorage, eptRetriever)
      k8sRefTracker  = FlinkK8sRefTracker(flinkConf, snapStorage, k8sOperator)
      clusterTrlEnvelop <- Sharding.messenger(ClusterTrk.Entity)
      k8sRefTrkEnvelope <- Sharding.messenger(K8sRefTrk.Entity)
    } yield TrackManagerLive(snapStorage.trackedList, clusterTracker, k8sRefTracker, clusterTrlEnvelop, k8sRefTrkEnvelope)
}

/**
 * Default implementation.
 */
class TrackManagerLive(
    stg: TrackedFcidStorage,
    clusterTracker: FlinkClusterTracker,
    k8sRefTracker: FlinkK8sRefTracker,
    clusterTrkEnvelope: Messenger[ClusterTrk.Cmd],
    k8sRefTrkEnvelope: Messenger[K8sRefTrk.Cmd])
    extends TrackManager {

  override private[potamoi] def registerEntities: URIO[Sharding with Scope, Unit] = {
    Sharding.registerEntity(ClusterTrk.Entity, clusterTracker.behavior, _ => Some(ClusterTrk.Stop)) *>
    Sharding.registerEntity(K8sRefTrk.Entity, k8sRefTracker.behavior, _ => Some(K8sRefTrk.Stop))
  }

  override def track(fcid: Fcid): IO[DataStorageErr, Unit] = {
    stg.put(fcid) *>
    clusterTrkEnvelope.sendDiscard(marshallFcid(fcid))(ClusterTrk.Start) *>
    k8sRefTrkEnvelope.sendDiscard(marshallFcid(fcid))(K8sRefTrk.Start)
  }

  override def untrack(fcid: Fcid): IO[DataStorageErr, Unit] = {
    stg.rm(fcid) *>
    clusterTrkEnvelope.sendDiscard(marshallFcid(fcid))(ClusterTrk.Stop) *>
    k8sRefTrkEnvelope.sendDiscard(marshallFcid(fcid))(K8sRefTrk.Stop)
  }

  override def isBeTracked(fcid: Fcid): IO[DataStorageErr, Boolean] = stg.exists(fcid)
  override def listTrackedClusters: Stream[DataStorageErr, Fcid]    = stg.list

  override def getTrackersStatus(fcid: Fcid): IO[Nothing, TrackersStatus] = {
    askStatus(clusterTrkEnvelope.send(marshallFcid(fcid))(ClusterTrk.IsStarted.apply)) <&>
    askStatus(k8sRefTrkEnvelope.send(marshallFcid(fcid))(K8sRefTrk.IsStarted.apply))
  } map { case (clusterTrk, k8sRefTrk) => TrackersStatus(fcid, clusterTrk, k8sRefTrk) }

  override def listTrackersStatus(parallelism: Int): Stream[DataStorageErr, TrackersStatus] =
    stg.list.mapZIOParUnordered(parallelism)(getTrackersStatus)

  private def askStatus(io: Task[Boolean]) =
    io.map(if _ then TrackerState.Running else TrackerState.Idle)
      .catchAll(e => ZIO.logError(e.getMessage) *> ZIO.succeed(TrackerState.Unknown))
}
