package potamoi.flink.storage.mem

import akka.actor.typed.Behavior
import potamoi.akka.{ActorCradle, DDataConf, LWWMapDData}
import potamoi.flink.{FlinkDataStoreErr, JobId}
import potamoi.flink.model.*
import potamoi.flink.storage.{JobMetricsStorage, JobOverviewStorage, JobSnapStorage, K8sRefSnapStorage}
import zio.ZIO
import zio.stream.ZStream

import scala.collection.mutable

/**
 * Flink job snapshot storage in-memory DData implementation.
 */
object JobSnapMemStorage:

  private object JobOvStg extends LWWMapDData[Fjid, FlinkJobOverview]("flink-job-overview-stg"):
    def apply(): Behavior[Req] = behavior(DDataConf.default)

  private object JobMetricStg extends LWWMapDData[Fjid, FlinkJobMetrics]("flink-job-metric-stg"):
    def apply(): Behavior[Req] = behavior(DDataConf.default)

  def make: ZIO[ActorCradle, Throwable, JobSnapStorage] = for {
    cradle           <- ZIO.service[ActorCradle]
    ovStg            <- cradle.spawn("flink-job-overview-store", JobOvStg())
    metricStg        <- cradle.spawn("flink-job-metric-store", JobMetricStg())
    given ActorCradle = cradle

  } yield new JobSnapStorage {
    val overview: JobOverviewStorage = new JobOverviewStorage:

      def put(ov: FlinkJobOverview): DIO[Unit]           = ovStg.put(ov.fjid, ov).uop
      def putAll(ovs: List[FlinkJobOverview]): DIO[Unit] = ovStg.puts(ovs.map(ov => ov.fjid -> ov)).uop
      def rm(fjid: Fjid): DIO[Unit]                      = ovStg.remove(fjid).uop
      def rm(fcid: Fcid): DIO[Unit]                      = ovStg.removeBySelectKey(_.fcid == fcid).uop

      def get(fjid: Fjid): DIO[Option[FlinkJobOverview]]      = ovStg.get(fjid).rop
      def list(fcid: Fcid): DIO[List[FlinkJobOverview]]       = ovStg.select { case (k, _) => k.fcid == fcid }.rop
      def listAll: DStream[FlinkJobOverview]                  = ZStream.fromIterableZIO(ovStg.listValues().rop)
      def listJobId(fcid: Fcid): DIO[List[Fjid]]              = ovStg.listKeys().map(_.filter(_.fcid == fcid)).rop
      def listAllJobId: DStream[Fjid]                         = ZStream.fromIterableZIO(ovStg.listKeys().rop)
      def getJobState(fjid: Fjid): DIO[Option[JobState]]      = ovStg.get(fjid).map(_.map(_.state)).rop
      def listJobState(fcid: Fcid): DIO[Map[JobId, JobState]] =
        ovStg.select { case (k, _) => k.fcid == fcid }.map(_.map(e => e.jobId -> e.state).toMap).rop

    val metrics: JobMetricsStorage = new JobMetricsStorage:
      def put(metric: FlinkJobMetrics): DIO[Unit]       = metricStg.put(Fjid(metric.clusterId, metric.namespace, metric.jobId), metric).uop
      def rm(fjid: Fjid): DIO[Unit]                     = metricStg.remove(fjid).uop
      def rm(fcid: Fcid): DIO[Unit]                     = metricStg.removeBySelectKey(_.fcid == fcid).uop
      def get(fjid: Fjid): DIO[Option[FlinkJobMetrics]] = metricStg.get(fjid).rop
      def list(fcid: Fcid): DIO[List[FlinkJobMetrics]]  = metricStg.select { case (k, _) => k.fcid == fcid }.rop
      def listJobId(fcid: Fcid): DIO[List[Fjid]]        = metricStg.listKeys().map(_.filter(_.fcid == fcid)).rop
  }
