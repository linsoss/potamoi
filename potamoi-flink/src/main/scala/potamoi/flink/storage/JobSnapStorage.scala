package potamoi.flink.storage

import potamoi.flink.model.{Fcid, Fjid, FlinkJobMetrics, FlinkJobOverview, JobState}
import potamoi.flink.{DataStorageErr, JobId}
import zio.IO
import zio.stream.Stream

/**
 * Flink job snapshot storage.
 */
trait JobSnapStorage:
  def overview: JobOverviewStorage
  def metrics: JobMetricsStorage

/**
 * Storage for flink job overview.
 */
trait JobOverviewStorage extends JobOverviewStorage.Modify with JobOverviewStorage.Query

object JobOverviewStorage {
  trait Modify:
    def put(ov: FlinkJobOverview): IO[DataStorageErr, Unit]
    def rm(fjid: Fjid): IO[DataStorageErr, Unit]
    def rm(fcid: Fcid): IO[DataStorageErr, Unit]

  trait Query:
    def get(fjid: Fjid): IO[DataStorageErr, Option[FlinkJobOverview]]
    def list(fcid: Fcid): IO[DataStorageErr, List[FlinkJobOverview]]
    def listAll: Stream[DataStorageErr, FlinkJobOverview]

    def listJobId(fcid: Fcid): IO[DataStorageErr, List[Fjid]]
    def listAllJobId: Stream[DataStorageErr, Fjid]

    def getJobState(fjid: Fjid): IO[DataStorageErr, Option[JobState]]
    def listJobState(fcid: Fcid): IO[DataStorageErr, Map[JobId, JobState]]
}

/**
 * Storage for flink job metrics.
 */
trait JobMetricsStorage extends JobMetricsStorage.Modify with JobMetricsStorage.Query

object JobMetricsStorage {
  trait Modify:
    def put(metric: FlinkJobMetrics): IO[DataStorageErr, Unit]
    def rm(fjid: Fjid): IO[DataStorageErr, Unit]
    def rm(fcid: Fcid): IO[DataStorageErr, Unit]

  trait Query:
    def get(fjid: Fjid): IO[DataStorageErr, Option[FlinkJobMetrics]]
    def list(fcid: Fcid): IO[DataStorageErr, List[FlinkJobMetrics]]
}
