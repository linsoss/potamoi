package potamoi.flink.storage.mem

import akka.actor.typed.Behavior
import akka.cluster.ddata.LWWMap
import potamoi.akka.{ActorCradle, DDataConf, LWWMapDData, ORSetDData}
import potamoi.flink.FlinkDataStoreErr
import potamoi.flink.model.*
import potamoi.flink.storage.*
import potamoi.KryoSerializable
import zio.stream.{Stream, ZSink, ZStream}
import zio.ZIO

import scala.collection.mutable

/**
 * Flink k8s resource snapshot storage in-memory DData implementation.
 */
object K8sRefSnapMemStorage:

  case class K8sRsKey(fcid: Fcid, name: String) extends KryoSerializable
  given Conversion[(Fcid, String), K8sRsKey] = (t: (Fcid, String)) => K8sRsKey(t._1, t._2)

  private object DeploySnapStg extends LWWMapDData[K8sRsKey, FlinkK8sDeploymentSnap]("flink-k8s-deploy-snap-stg"):
    def apply(): Behavior[Req] = behavior(DDataConf.default)

  private object SvcSnapStg extends LWWMapDData[K8sRsKey, FlinkK8sServiceSnap]("flink-k8s-svc-snap-stg"):
    def apply(): Behavior[Req] = behavior(DDataConf.default)

  private object PodSnapStg extends LWWMapDData[K8sRsKey, FlinkK8sPodSnap]("flink-k8s-pod-snap-stg"):
    def apply(): Behavior[Req] = behavior(DDataConf.default)

  private object PodMetricsStg extends LWWMapDData[K8sRsKey, FlinkK8sPodMetrics]("flink-k8s-pod-metrics-stg"):
    def apply(): Behavior[Req] = behavior(DDataConf.default)

  private object ConfigmapNamesStg extends ORSetDData[K8sRsKey]("flink-k8s-configmap-names-stg"):
    def apply(): Behavior[Req] = behavior(DDataConf.default)

  def make: ZIO[ActorCradle, Throwable, K8sRefSnapStorage] = for {
    cradle           <- ZIO.service[ActorCradle]
    deploySnapStg    <- cradle.spawn("flink-k8s-deploy-snap-store", DeploySnapStg())
    svcSnapStg       <- cradle.spawn("flink-k8s-svc-snap-store", SvcSnapStg())
    podSnapStg       <- cradle.spawn("flink-k8s-pod-snap-store", PodSnapStg())
    podMetricsStg    <- cradle.spawn("flink-k8s-pod-metrics-store", PodMetricsStg())
    configmapStg     <- cradle.spawn("flink-k8s-configmap-names-store", ConfigmapNamesStg())
    given ActorCradle = cradle

  } yield new K8sRefSnapStorage {

    val deployment: K8sDeploymentSnapStorage = new K8sDeploymentSnapStorage:
      def put(snap: FlinkK8sDeploymentSnap): DIO[Unit]                             = deploySnapStg.put(snap.fcid -> snap.name, snap).uop
      def rm(fcid: Fcid, deployName: String): DIO[Unit]                            = deploySnapStg.remove(fcid -> deployName).uop
      def rm(fcid: Fcid): DIO[Unit]                                                = deploySnapStg.removeBySelectKey(_.fcid == fcid).uop
      def get(fcid: Fcid, deployName: String): DIO[Option[FlinkK8sDeploymentSnap]] = deploySnapStg.get(fcid -> deployName).rop
      def list(fcid: Fcid): DIO[List[FlinkK8sDeploymentSnap]]                      = deploySnapStg.listValues().rop
      def listName(fcid: Fcid): DIO[List[String]]                                  = deploySnapStg.listKeys().map(_.map(_.name)).rop

    val service: K8sServiceSnapStorage = new K8sServiceSnapStorage:
      def put(snap: FlinkK8sServiceSnap): DIO[Unit]                          = svcSnapStg.put(snap.fcid -> snap.name, snap).uop
      def rm(fcid: Fcid, svcName: String): DIO[Unit]                         = svcSnapStg.remove(fcid -> svcName).uop
      def rm(fcid: Fcid): DIO[Unit]                                          = svcSnapStg.removeBySelectKey(_.fcid == fcid).uop
      def get(fcid: Fcid, svcName: String): DIO[Option[FlinkK8sServiceSnap]] = svcSnapStg.get(fcid -> svcName).rop
      def list(fcid: Fcid): DIO[List[FlinkK8sServiceSnap]]                   = svcSnapStg.listValues().rop
      def listName(fcid: Fcid): DIO[List[String]]                            = svcSnapStg.listKeys().map(_.map(_.name)).rop

    val pod: K8sPodSnapStorage = new K8sPodSnapStorage:
      def put(snap: FlinkK8sPodSnap): DIO[Unit]                          = podSnapStg.put(snap.fcid -> snap.name, snap).uop
      def rm(fcid: Fcid, podName: String): DIO[Unit]                     = podSnapStg.remove(fcid -> podName).uop
      def rm(fcid: Fcid): DIO[Unit]                                      = podSnapStg.removeBySelectKey(_.fcid == fcid).uop
      def get(fcid: Fcid, podName: String): DIO[Option[FlinkK8sPodSnap]] = podSnapStg.get(fcid -> podName).rop
      def list(fcid: Fcid): DIO[List[FlinkK8sPodSnap]]                   = podSnapStg.listValues().rop
      def listName(fcid: Fcid): DIO[List[String]]                        = podSnapStg.listKeys().map(_.map(_.name)).rop

    val podMetrics: K8sPodMetricsStorage = new K8sPodMetricsStorage:
      def put(snap: FlinkK8sPodMetrics): DIO[Unit]                          = podMetricsStg.put(snap.fcid -> snap.name, snap).uop
      def rm(fcid: Fcid, podName: String): DIO[Unit]                        = podMetricsStg.remove(fcid -> podName).uop
      def rm(fcid: Fcid): DIO[Unit]                                         = podMetricsStg.removeBySelectKey(_.fcid == fcid).uop
      def get(fcid: Fcid, podName: String): DIO[Option[FlinkK8sPodMetrics]] = podMetricsStg.get(fcid -> podName).rop
      def list(fcid: Fcid): DIO[List[FlinkK8sPodMetrics]]                   = podMetricsStg.listValues().rop
      def listName(fcid: Fcid): DIO[List[String]]                           = podMetricsStg.listKeys().map(_.map(_.name)).rop

    val configmap: K8sConfigmapNamesStorage = new K8sConfigmapNamesStorage:
      def put(fcid: Fcid, configmapName: String): DIO[Unit] = configmapStg.put(fcid -> configmapName).uop
      def rm(fcid: Fcid, configmapName: String): DIO[Unit]  = configmapStg.remove(fcid -> configmapName).uop
      def rm(fcid: Fcid): DIO[Unit]                         = configmapStg.list().map(_.filter(_.fcid == fcid)).flatMap(configmapStg.removes(_)).uop
      def listName(fcid: Fcid): DIO[List[String]]           = configmapStg.list().map(_.map(_.name).toList).rop
  }
