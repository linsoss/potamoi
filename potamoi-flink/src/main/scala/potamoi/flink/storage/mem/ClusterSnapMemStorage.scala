package potamoi.flink.storage.mem

import akka.actor.typed.Behavior
import potamoi.akka.{AkkaMatrix, DDataConf, LWWMapDData}
import potamoi.flink.FlinkDataStoreErr
import potamoi.flink.model.*
import potamoi.flink.storage.*
import zio.{stream, IO, Ref, UIO, ULayer, ZIO, ZLayer}
import zio.stream.{Stream, ZSink, ZStream}

import scala.collection.mutable

/**
 * Flink cluster snapshot storage in-memory DData implementation.
 */
object ClusterSnapMemStorage:

  private object ClusterOvStg extends LWWMapDData[Fcid, FlinkClusterOverview]("flink-cluster-ov-stg"):
    def apply(): Behavior[Req] = behavior(DDataConf.default)

  private object JmMetricsStg extends LWWMapDData[Fcid, FlinkJmMetrics]("flink-jm-metric-stg"):
    def apply(): Behavior[Req] = behavior(DDataConf.default)

  private object TmDetailStg extends LWWMapDData[Ftid, FlinkTmDetail]("flink-tm-detail-stg"):
    def apply(): Behavior[Req] = behavior(DDataConf.default)

  private object TmMetricStg extends LWWMapDData[Ftid, FlinkTmMetrics]("flink-tm-metric-stg"):
    def apply(): Behavior[Req] = behavior(DDataConf.default)

  def make: ZIO[AkkaMatrix, Throwable, ClusterSnapStorage] = for {
    matrix           <- ZIO.service[AkkaMatrix]
    clusterOvStg     <- matrix.spawn("flink-cluster-ov-store", ClusterOvStg())
    jmMetricStg      <- matrix.spawn("flink-jm-metric-store", JmMetricsStg())
    tmDetailStg      <- matrix.spawn("flink-tm-detail-store", TmDetailStg())
    tmMetricStg      <- matrix.spawn("flink-tm-metric-store", TmMetricStg())
    given AkkaMatrix = matrix

  } yield new ClusterSnapStorage {

    val overview: ClusterOverviewStorage = new ClusterOverviewStorage:
      def get(fcid: Fcid): DIO[Option[FlinkClusterOverview]] = clusterOvStg.get(fcid).uop
      def listAll: DStream[FlinkClusterOverview]             = ZStream.fromIterableZIO(clusterOvStg.listValues().rop)
      def put(ov: FlinkClusterOverview): DIO[Unit]           = clusterOvStg.put(ov.fcid, ov).uop
      def rm(fcid: Fcid): DIO[Unit]                          = clusterOvStg.remove(fcid).uop

    val jmMetrics: JmMetricsStorage = new JmMetricsStorage:
      def get(fcid: Fcid): DIO[Option[FlinkJmMetrics]] = jmMetricStg.get(fcid).rop
      def put(metric: FlinkJmMetrics): DIO[Unit]       = jmMetricStg.put(Fcid(metric.clusterId, metric.namespace), metric).uop
      def rm(fcid: Fcid): DIO[Unit]                    = jmMetricStg.remove(fcid).uop

    val tmDetail: TmDetailStorage = new TmDetailStorage:
      def put(tm: FlinkTmDetail): DIO[Unit]           = tmDetailStg.put(tm.ftid, tm).uop
      def putAll(tm: List[FlinkTmDetail]): DIO[Unit]  = tmDetailStg.puts(tm.map(tm => tm.ftid -> tm)).uop
      def rm(ftid: Ftid): DIO[Unit]                   = tmDetailStg.remove(ftid).uop
      def rm(fcid: Fcid): DIO[Unit]                   = tmDetailStg.removeBySelectKey(_.fcid == fcid).uop
      def get(ftid: Ftid): DIO[Option[FlinkTmDetail]] = tmDetailStg.get(ftid).rop
      def list(fcid: Fcid): DIO[List[FlinkTmDetail]]  = tmDetailStg.select { case (k, _) => k.fcid == fcid }.rop
      def listAll: DStream[FlinkTmDetail]             = ZStream.fromIterableZIO(tmDetailStg.listValues().rop)
      def listTmId(fcid: Fcid): DIO[List[Ftid]]       = tmDetailStg.listKeys().map(_.filter(_.fcid == fcid)).rop
      def listAllTmId: DStream[Ftid]                  = ZStream.fromIterableZIO(tmDetailStg.listKeys().rop)

    val tmMetrics: TmMetricStorage = new TmMetricStorage:
      def put(metric: FlinkTmMetrics): DIO[Unit]       = tmMetricStg.put(Ftid(metric.clusterId, metric.namespace, metric.tmId), metric).uop
      def rm(ftid: Ftid): DIO[Unit]                    = tmMetricStg.remove(ftid).uop
      def rm(fcid: Fcid): DIO[Unit]                    = tmMetricStg.removeBySelectKey(_.fcid == fcid).uop
      def get(ftid: Ftid): DIO[Option[FlinkTmMetrics]] = tmMetricStg.get(ftid).rop
      def list(fcid: Fcid): DIO[List[FlinkTmMetrics]]  = tmMetricStg.select { case (k, _) => k.fcid == fcid }.rop
      def listTmId(fcid: Fcid): DIO[List[Ftid]]        = tmMetricStg.listKeys().map(_.filter(_.fcid == fcid)).rop
  }
