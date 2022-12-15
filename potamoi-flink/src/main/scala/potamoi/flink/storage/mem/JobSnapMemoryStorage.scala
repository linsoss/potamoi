package potamoi.flink.storage.mem

import potamoi.flink.model.{Fcid, Fjid, FlinkJobMetrics, FlinkJobOverview, JobState}
import potamoi.flink.storage.{JobMetricsStorage, JobOverviewStorage, JobSnapStorage}
import potamoi.flink.{DataStorageErr, JobId}
import zio.Ref
import zio.{stream, IO, Ref, UIO, ULayer, ZLayer}
import zio.stream.{Stream, ZSink, ZStream}

import scala.collection.mutable

/**
 * Flink job snapshot storage in-memory implementation.
 */
object JobSnapMemoryStorage:
  def instance: UIO[JobSnapStorage] =
    for {
      ovRef     <- Ref.make(mutable.Map.empty[Fjid, FlinkJobOverview])
      metricRef <- Ref.make(mutable.Map.empty[Fjid, FlinkJobMetrics])
    } yield new JobSnapStorage:
      lazy val overview: JobOverviewStorage = JobOverviewMemoryStorage(ovRef)
      lazy val metrics: JobMetricsStorage   = JobMetricsMemoryStorage(metricRef)

class JobOverviewMemoryStorage(ref: Ref[mutable.Map[Fjid, FlinkJobOverview]]) extends JobOverviewStorage:
  private val stg                                                        = MapBasedStg(ref)
  def put(ov: FlinkJobOverview): IO[DataStorageErr, Unit]                = stg.put(ov.fjid, ov)
  def rm(fjid: Fjid): IO[DataStorageErr, Unit]                           = stg.delete(fjid)
  def rm(fcid: Fcid): IO[DataStorageErr, Unit]                           = stg.deleteByKey(_.fcid == fcid)
  def get(fjid: Fjid): IO[DataStorageErr, Option[FlinkJobOverview]]      = stg.get(fjid)
  def list(fcid: Fcid): IO[DataStorageErr, List[FlinkJobOverview]]       = stg.getByKey(_.fcid == fcid)
  def listAll: Stream[DataStorageErr, FlinkJobOverview]                  = stg.streamValues
  def listJobId(fcid: Fcid): IO[DataStorageErr, List[Fjid]]              = stg.getPartByKey(_.fcid == fcid, _.fjid)
  def listAllJobId: Stream[DataStorageErr, Fjid]                         = stg.streamValues.map(_.fjid)
  def getJobState(fjid: Fjid): IO[DataStorageErr, Option[JobState]]      = stg.getPart(fjid, _.state)
  def listJobState(fcid: Fcid): IO[DataStorageErr, Map[JobId, JobState]] = stg.getByKey(_.fcid == fcid).map(_.map(ov => ov.jobId -> ov.state).toMap)

case class JobMetricsMemoryStorage(ref: Ref[mutable.Map[Fjid, FlinkJobMetrics]]) extends JobMetricsStorage:
  private val stg                                                  = MapBasedStg(ref)
  def put(metric: FlinkJobMetrics): IO[DataStorageErr, Unit]       = stg.put(Fjid(metric.clusterId, metric.namespace, metric.jobId), metric)
  def rm(fjid: Fjid): IO[DataStorageErr, Unit]                     = stg.delete(fjid)
  def rm(fcid: Fcid): IO[DataStorageErr, Unit]                     = stg.deleteByKey(_.fcid == fcid)
  def get(fjid: Fjid): IO[DataStorageErr, Option[FlinkJobMetrics]] = stg.get(fjid)
  def list(fcid: Fcid): IO[DataStorageErr, List[FlinkJobMetrics]]  = stg.getByKey(_.fcid == fcid)
