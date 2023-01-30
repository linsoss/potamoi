package potamoi.flink.storage

import potamoi.akka.ActorCradle
import potamoi.flink.FlinkDataStoreErr
import potamoi.flink.model.*
import potamoi.flink.storage.mem.*
import potamoi.EarlyLoad
import zio.{IO, URIO, ZIO, ZLayer}
import zio.stream.Stream

/**
 * Flink data storage.
 */
trait FlinkDataStorage:

  def trackedList: TrackedFcidStorage
  def restEndpoint: RestEndpointStorage
  def restProxy: RestProxyFcidStorage
  def cluster: ClusterSnapStorage
  def job: JobSnapStorage
  def k8sRef: K8sRefSnapStorage
  def interact: InteractSessionStorage

  /**
   * Remove all current snapshot data belongs to Fcid.
   */
  def rmSnapData(fcid: Fcid): IO[FlinkDataStoreErr, Unit] = {
    restEndpoint.rm(fcid) <&>
    restProxy.rm(fcid) <&>
    cluster.rmSnapData(fcid) <&>
    job.rmSnapData(fcid) <&>
    k8sRef.rmSnapData(fcid)
  }

object FlinkDataStorage extends EarlyLoad[FlinkDataStorage]:

  val test = memory

  /**
   * In-memory Akka DData storage.
   */
  lazy val memory: ZLayer[ActorCradle, Throwable, FlinkDataStorage] = ZLayer {
    for {
      trackedLists  <- TrackedFcidMemStorage.make
      restEndpoints <- RestEndpointMemStorage.make
      restProxies   <- RestProxyFcidMemStorage.make
      clusters      <- ClusterSnapMemStorage.make
      jobs          <- JobSnapMemStorage.make
      k8sRefs       <- K8sRefSnapMemStorage.make
      interacts     <- InteractSessionMemStorage.make
    } yield new FlinkDataStorage:
      val trackedList  = trackedLists
      val restEndpoint = restEndpoints
      val restProxy    = restProxies
      val cluster      = clusters
      val job          = jobs
      val k8sRef       = k8sRefs
      val interact     = interacts
  }

  override def active: URIO[FlinkDataStorage, Unit] = ZIO.service[FlinkDataStorage].unit
