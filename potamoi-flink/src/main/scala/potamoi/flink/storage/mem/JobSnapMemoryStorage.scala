package potamoi.flink.storage.mem

import potamoi.flink.{FlinkDataStoreErr, JobId}
import potamoi.flink.model.*
import potamoi.flink.storage.{JobMetricsStorage, JobOverviewStorage, JobSnapStorage}
import zio.{stream, IO, Ref, UIO, ULayer, ZLayer}
import zio.stream.{Stream, ZSink, ZStream}

import scala.collection.mutable

/**
 * Flink job snapshot storage in-memory implementation.
 */
object JobSnapMemoryStorage:
  def make: UIO[JobSnapStorage] =
    for {
      ovRef     <- Ref.make(mutable.Map.empty[Fjid, FlinkJobOverview])
      metricRef <- Ref.make(mutable.Map.empty[Fjid, FlinkJobMetrics])
    } yield new JobSnapStorage:
      lazy val overview: JobOverviewStorage = JobOverviewMemoryStorage(ovRef)
      lazy val metrics: JobMetricsStorage   = JobMetricsMemoryStorage(metricRef)

class JobOverviewMemoryStorage(ref: Ref[mutable.Map[Fjid, FlinkJobOverview]]) extends JobOverviewStorage:
  private val stg                                                        = MapBasedStg(ref)
  def put(ov: FlinkJobOverview): IO[FlinkDataStoreErr, Unit]                = stg.put(ov.fjid, ov)
  def putAll(ovs: List[FlinkJobOverview]): IO[FlinkDataStoreErr, Unit]      = stg.putAll(ovs.map(ov => ov.fjid -> ov).toMap)
  def rm(fjid: Fjid): IO[FlinkDataStoreErr, Unit]                           = stg.delete(fjid)
  def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]                           = stg.deleteByKey(_.fcid == fcid)
  def get(fjid: Fjid): IO[FlinkDataStoreErr, Option[FlinkJobOverview]]      = stg.get(fjid)
  def list(fcid: Fcid): IO[FlinkDataStoreErr, List[FlinkJobOverview]]       = stg.getByKey(_.fcid == fcid)
  def listAll: Stream[FlinkDataStoreErr, FlinkJobOverview]                  = stg.streamValues
  def listJobId(fcid: Fcid): IO[FlinkDataStoreErr, List[Fjid]]              = stg.getPartByKey(_.fcid == fcid, _.fjid)
  def listAllJobId: Stream[FlinkDataStoreErr, Fjid]                         = stg.streamValues.map(_.fjid)
  def getJobState(fjid: Fjid): IO[FlinkDataStoreErr, Option[JobState]]      = stg.getPart(fjid, _.state)
  def listJobState(fcid: Fcid): IO[FlinkDataStoreErr, Map[JobId, JobState]] = stg.getByKey(_.fcid == fcid).map(_.map(ov => ov.jobId -> ov.state).toMap)

case class JobMetricsMemoryStorage(ref: Ref[mutable.Map[Fjid, FlinkJobMetrics]]) extends JobMetricsStorage:
  private val stg                                                  = MapBasedStg(ref)
  def put(metric: FlinkJobMetrics): IO[FlinkDataStoreErr, Unit]       = stg.put(Fjid(metric.clusterId, metric.namespace, metric.jobId), metric)
  def rm(fjid: Fjid): IO[FlinkDataStoreErr, Unit]                     = stg.delete(fjid)
  def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]                     = stg.deleteByKey(_.fcid == fcid)
  def get(fjid: Fjid): IO[FlinkDataStoreErr, Option[FlinkJobMetrics]] = stg.get(fjid)
  def list(fcid: Fcid): IO[FlinkDataStoreErr, List[FlinkJobMetrics]]  = stg.getByKey(_.fcid == fcid)
  def listJobId(fcid: Fcid): IO[FlinkDataStoreErr, List[Fjid]]        = stg.getKeys.map(_.filter(_.fcid == fcid))
