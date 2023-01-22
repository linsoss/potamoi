package potamoi.flink.storage.mem

import potamoi.flink.FlinkDataStoreErr
import potamoi.flink.model.*
import potamoi.flink.storage.*
import zio.{stream, IO, Ref, UIO, ULayer, ZLayer}
import zio.stream.{Stream, ZSink, ZStream}

import scala.collection.mutable

/**
 * Flink k8s resource snapshot storage in-memory implementation.
 */
object K8sRefSnapMemoryStorage:
  def instance: UIO[K8sRefSnapStorage] =
    for {
      deployRef    <- Ref.make(mutable.Map.empty[(Fcid, String), FlinkK8sDeploymentSnap])
      svcRef       <- Ref.make(mutable.Map.empty[(Fcid, String), FlinkK8sServiceSnap])
      podRef       <- Ref.make(mutable.Map.empty[(Fcid, String), FlinkK8sPodSnap])
      podMetricRef <- Ref.make(mutable.Map.empty[(Fcid, String), FlinkK8sPodMetrics])
      configmapRef <- Ref.make(mutable.Set.empty[(Fcid, String)])
    } yield new K8sRefSnapStorage:
      lazy val deployment: K8sDeploymentSnapStorage = K8sDeploymentSnapMemoryStorage(deployRef)
      lazy val service: K8sServiceSnapStorage       = K8sServiceSnapMemoryStorage(svcRef)
      lazy val pod: K8sPodSnapStorage               = K8sPodSnapMemoryStorage(podRef)
      lazy val podMetrics: K8sPodMetricsStorage     = K8sPodMetricsMemoryStorage(podMetricRef)
      lazy val configmap: K8sConfigmapNamesStorage  = K8sConfigmapNamesMemoryStorage(configmapRef)

class K8sDeploymentSnapMemoryStorage(ref: Ref[mutable.Map[(Fcid, String), FlinkK8sDeploymentSnap]]) extends K8sDeploymentSnapStorage:
  private val stg                                                                             = MapBasedStg(ref)
  def put(snap: FlinkK8sDeploymentSnap): IO[FlinkDataStoreErr, Unit]                             = stg.put(snap.fcid -> snap.name, snap)
  def rm(fcid: Fcid, deployName: String): IO[FlinkDataStoreErr, Unit]                            = stg.delete(fcid -> deployName)
  def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]                                                = stg.deleteByKey(_._1 == fcid)
  def get(fcid: Fcid, deployName: String): IO[FlinkDataStoreErr, Option[FlinkK8sDeploymentSnap]] = stg.get(fcid -> deployName)
  def list(fcid: Fcid): IO[FlinkDataStoreErr, List[FlinkK8sDeploymentSnap]]                      = stg.getValues
  def listName(fcid: Fcid): IO[FlinkDataStoreErr, List[String]]                                  = stg.getKeys.map(_.map(_._2))

class K8sServiceSnapMemoryStorage(ref: Ref[mutable.Map[(Fcid, String), FlinkK8sServiceSnap]]) extends K8sServiceSnapStorage:
  private val stg                                                                       = MapBasedStg(ref)
  def put(snap: FlinkK8sServiceSnap): IO[FlinkDataStoreErr, Unit]                          = stg.put(snap.fcid -> snap.name, snap)
  def rm(fcid: Fcid, svcName: String): IO[FlinkDataStoreErr, Unit]                         = stg.delete(fcid -> svcName)
  def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]                                          = stg.deleteByKey(_._1 == fcid)
  def get(fcid: Fcid, svcName: String): IO[FlinkDataStoreErr, Option[FlinkK8sServiceSnap]] = stg.get(fcid -> svcName)
  def list(fcid: Fcid): IO[FlinkDataStoreErr, List[FlinkK8sServiceSnap]]                   = stg.getValues
  def listName(fcid: Fcid): IO[FlinkDataStoreErr, List[String]]                            = stg.getKeys.map(_.map(_._2))

class K8sPodSnapMemoryStorage(ref: Ref[mutable.Map[(Fcid, String), FlinkK8sPodSnap]]) extends K8sPodSnapStorage:
  private val stg                                                                   = MapBasedStg(ref)
  def put(snap: FlinkK8sPodSnap): IO[FlinkDataStoreErr, Unit]                          = stg.put(snap.fcid -> snap.name, snap)
  def rm(fcid: Fcid, podName: String): IO[FlinkDataStoreErr, Unit]                     = stg.delete(fcid -> podName)
  def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]                                      = stg.deleteByKey(_._1 == fcid)
  def get(fcid: Fcid, podName: String): IO[FlinkDataStoreErr, Option[FlinkK8sPodSnap]] = stg.get(fcid -> podName)
  def list(fcid: Fcid): IO[FlinkDataStoreErr, List[FlinkK8sPodSnap]]                   = stg.getValues
  def listName(fcid: Fcid): IO[FlinkDataStoreErr, List[String]]                        = stg.getKeys.map(_.map(_._2))

class K8sPodMetricsMemoryStorage(ref: Ref[mutable.Map[(Fcid, String), FlinkK8sPodMetrics]]) extends K8sPodMetricsStorage:
  private val stg                                                                      = MapBasedStg(ref)
  def put(snap: FlinkK8sPodMetrics): IO[FlinkDataStoreErr, Unit]                          = stg.put(snap.fcid -> snap.name, snap)
  def rm(fcid: Fcid, podName: String): IO[FlinkDataStoreErr, Unit]                        = stg.delete(fcid -> podName)
  def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]                                         = stg.deleteByKey(_._1 == fcid)
  def get(fcid: Fcid, podName: String): IO[FlinkDataStoreErr, Option[FlinkK8sPodMetrics]] = stg.get(fcid -> podName)
  def list(fcid: Fcid): IO[FlinkDataStoreErr, List[FlinkK8sPodMetrics]]                   = stg.getValues
  def listName(fcid: Fcid): IO[FlinkDataStoreErr, List[String]]                           = stg.getKeys.map(_.map(_._2))

class K8sConfigmapNamesMemoryStorage(ref: Ref[mutable.Set[(Fcid, String)]]) extends K8sConfigmapNamesStorage:
  def put(fcid: Fcid, configmapName: String): IO[FlinkDataStoreErr, Unit] = ref.update(_ += (fcid -> configmapName))
  def rm(fcid: Fcid, configmapName: String): IO[FlinkDataStoreErr, Unit]  = ref.update(_ -= (fcid -> configmapName))
  def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]                         = ref.update(set => set.filter(_._1 == fcid).foldLeft(set)((ac, c) => ac -= c))
  def listName(fcid: Fcid): IO[FlinkDataStoreErr, List[String]]           = ref.get.map(_.filter(_._1 == fcid).map(_._2).toList)
