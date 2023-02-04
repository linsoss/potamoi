package potamoi.flink.storage

import potamoi.flink.{FlinkDataStoreErr, JobId}
import potamoi.flink.model.*
import potamoi.flink.model.snapshot.{FlinkJobMetrics, FlinkJobOverview, JobState}
import zio.IO
import zio.stream.Stream

/**
 * Flink job snapshot storage.
 */
trait JobSnapStorage:
  def overview: JobOverviewStorage
  def metrics: JobMetricsStorage

  def rmSnapData(fcid: Fcid): IO[FlinkDataStoreErr, Unit] = {
    overview.rm(fcid) <&>
    metrics.rm(fcid)
  }

/**
 * Storage for flink job overview.
 */
trait JobOverviewStorage extends JobOverviewStorage.Modify with JobOverviewStorage.Query

object JobOverviewStorage {
  trait Modify:
    def put(ov: FlinkJobOverview): IO[FlinkDataStoreErr, Unit]
    def putAll(ovs: List[FlinkJobOverview]): IO[FlinkDataStoreErr, Unit]
    def rm(fjid: Fjid): IO[FlinkDataStoreErr, Unit]
    def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]

  trait Query:
    def get(fjid: Fjid): IO[FlinkDataStoreErr, Option[FlinkJobOverview]]
    def list(fcid: Fcid): IO[FlinkDataStoreErr, List[FlinkJobOverview]]
    def listAll: Stream[FlinkDataStoreErr, FlinkJobOverview]

    def listJobId(fcid: Fcid): IO[FlinkDataStoreErr, List[Fjid]]
    def listAllJobId: Stream[FlinkDataStoreErr, Fjid]

    def getJobState(fjid: Fjid): IO[FlinkDataStoreErr, Option[JobState]]
    def listJobState(fcid: Fcid): IO[FlinkDataStoreErr, Map[JobId, JobState]]
}

/**
 * Storage for flink job metrics.
 */
trait JobMetricsStorage extends JobMetricsStorage.Modify with JobMetricsStorage.Query

object JobMetricsStorage {
  trait Modify:
    def put(metric: FlinkJobMetrics): IO[FlinkDataStoreErr, Unit]
    def rm(fjid: Fjid): IO[FlinkDataStoreErr, Unit]
    def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]

  trait Query:
    def get(fjid: Fjid): IO[FlinkDataStoreErr, Option[FlinkJobMetrics]]
    def list(fcid: Fcid): IO[FlinkDataStoreErr, List[FlinkJobMetrics]]
    def listJobId(fcid: Fcid): IO[FlinkDataStoreErr, List[Fjid]]
}
