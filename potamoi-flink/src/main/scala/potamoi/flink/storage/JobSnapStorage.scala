package potamoi.flink.storage

import potamoi.flink.{DataStoreErr, JobId}
import potamoi.flink.model.*
import zio.IO
import zio.stream.Stream

/**
 * Flink job snapshot storage.
 */
trait JobSnapStorage:
  def overview: JobOverviewStorage
  def metrics: JobMetricsStorage

  def rmSnapData(fcid: Fcid): IO[DataStoreErr, Unit] = {
    overview.rm(fcid) <&>
    metrics.rm(fcid)
  }

/**
 * Storage for flink job overview.
 */
trait JobOverviewStorage extends JobOverviewStorage.Modify with JobOverviewStorage.Query

object JobOverviewStorage {
  trait Modify:
    def put(ov: FlinkJobOverview): IO[DataStoreErr, Unit]
    def putAll(ovs: List[FlinkJobOverview]): IO[DataStoreErr, Unit]
    def rm(fjid: Fjid): IO[DataStoreErr, Unit]
    def rm(fcid: Fcid): IO[DataStoreErr, Unit]

  trait Query:
    def get(fjid: Fjid): IO[DataStoreErr, Option[FlinkJobOverview]]
    def list(fcid: Fcid): IO[DataStoreErr, List[FlinkJobOverview]]
    def listAll: Stream[DataStoreErr, FlinkJobOverview]

    def listJobId(fcid: Fcid): IO[DataStoreErr, List[Fjid]]
    def listAllJobId: Stream[DataStoreErr, Fjid]

    def getJobState(fjid: Fjid): IO[DataStoreErr, Option[JobState]]
    def listJobState(fcid: Fcid): IO[DataStoreErr, Map[JobId, JobState]]
}

/**
 * Storage for flink job metrics.
 */
trait JobMetricsStorage extends JobMetricsStorage.Modify with JobMetricsStorage.Query

object JobMetricsStorage {
  trait Modify:
    def put(metric: FlinkJobMetrics): IO[DataStoreErr, Unit]
    def rm(fjid: Fjid): IO[DataStoreErr, Unit]
    def rm(fcid: Fcid): IO[DataStoreErr, Unit]

  trait Query:
    def get(fjid: Fjid): IO[DataStoreErr, Option[FlinkJobMetrics]]
    def list(fcid: Fcid): IO[DataStoreErr, List[FlinkJobMetrics]]
    def listJobId(fcid: Fcid): IO[DataStoreErr, List[Fjid]]
}
