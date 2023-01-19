package potamoi.flink.observer.tracker

import com.devsisters.shardcake.{Messenger, Sharding}
import potamoi.flink.{DataStoreErr, FlinkConf, FlinkErr, FlinkRestEndpointRetriever}
import potamoi.flink.model.Fcid
import potamoi.flink.storage.{FlinkDataStorage, TrackedFcidStorage}
import potamoi.flink.FlinkErr.FailToConnectShardEntity
import potamoi.kubernetes.K8sOperator
import potamoi.sharding.ShardRegister
import zio.{IO, RIO, Scope, Task, UIO, URIO, ZIO, ZIOAspect}
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
                snapStorage: FlinkDataStorage,
                eptRetriever: FlinkRestEndpointRetriever,
                k8sOperator: K8sOperator): URIO[Sharding, TrackManagerLive] =
    for {
      _ <- ZIO.unit
      clusterTracker = FlinkClusterTracker(flinkConf, snapStorage, eptRetriever)
      k8sRefTracker  = FlinkK8sRefTracker(flinkConf, snapStorage, k8sOperator)
      clusterTrlEnvelop <- Sharding.messenger(ClusterTrk.Entity)
      k8sRefTrkEnvelope <- Sharding.messenger(K8sRefTrk.Entity)
    } yield TrackManagerLive(snapStorage, clusterTracker, k8sRefTracker, clusterTrlEnvelop, k8sRefTrkEnvelope)
}

/**
 * Default implementation.
 */
class TrackManagerLive(
                        snapStorage: FlinkDataStorage,
                        clusterTracker: FlinkClusterTracker,
                        k8sRefTracker: FlinkK8sRefTracker,
                        clusterTrkEnvelope: Messenger[ClusterTrk.Cmd],
                        k8sRefTrkEnvelope: Messenger[K8sRefTrk.Cmd])
    extends TrackManager {

  override private[potamoi] def registerEntities: URIO[Sharding with Scope, Unit] = {
    Sharding.registerEntity(ClusterTrk.Entity, clusterTracker.behavior, _ => Some(ClusterTrk.Terminate)) *>
    Sharding.registerEntity(K8sRefTrk.Entity, k8sRefTracker.behavior, _ => Some(K8sRefTrk.Terminate))
  }

  override def track(fcid: Fcid): IO[DataStoreErr | FailToConnectShardEntity | FlinkErr, Unit] = {
    snapStorage.trackedList.put(fcid) *>
    clusterTrkEnvelope
      .send(marshallFcid(fcid))(ClusterTrk.Start.apply)
      .mapError(FailToConnectShardEntity(ClusterTrk.Entity.name, _))
      .unit *>
    k8sRefTrkEnvelope
      .send(marshallFcid(fcid))(K8sRefTrk.Start.apply)
      .mapError(FailToConnectShardEntity(K8sRefTrk.Entity.name, _))
      .unit
  } @@ ZIOAspect.annotated(fcid.toAnno*)

  override def untrack(fcid: Fcid): IO[DataStoreErr | FailToConnectShardEntity | FlinkErr, Unit] = {
    snapStorage.trackedList.rm(fcid) *>
    clusterTrkEnvelope
      .send(marshallFcid(fcid))(ClusterTrk.Stop.apply)
      .mapError(FailToConnectShardEntity(ClusterTrk.Entity.name, _))
      .unit *>
    k8sRefTrkEnvelope
      .send(marshallFcid(fcid))(K8sRefTrk.Stop.apply)
      .mapError(FailToConnectShardEntity(K8sRefTrk.Entity.name, _))
      .unit *>
    // remove all snapshot data belongs to Fcid
    snapStorage.rmSnapData(fcid)
  } @@ ZIOAspect.annotated(fcid.toAnno*)

  override def isBeTracked(fcid: Fcid): IO[DataStoreErr, Boolean] = snapStorage.trackedList.exists(fcid)
  override def listTrackedClusters: Stream[DataStoreErr, Fcid]    = snapStorage.trackedList.list

  override def getTrackersStatus(fcid: Fcid): IO[Nothing, TrackersStatus] = {
    askStatus(clusterTrkEnvelope.send(marshallFcid(fcid))(ClusterTrk.IsStarted.apply)) <&>
    askStatus(k8sRefTrkEnvelope.send(marshallFcid(fcid))(K8sRefTrk.IsStarted.apply))
  } map { case (clusterTrk, k8sRefTrk) => TrackersStatus(fcid, clusterTrk, k8sRefTrk) }

  override def listTrackersStatus(parallelism: Int): Stream[DataStoreErr, TrackersStatus] =
    snapStorage.trackedList.list.mapZIOParUnordered(parallelism)(getTrackersStatus)

  private def askStatus(io: Task[Boolean]) =
    io.map(if _ then TrackerState.Running else TrackerState.Idle)
      .catchAll(e => ZIO.logError(e.getMessage) *> ZIO.succeed(TrackerState.Unknown))
}
