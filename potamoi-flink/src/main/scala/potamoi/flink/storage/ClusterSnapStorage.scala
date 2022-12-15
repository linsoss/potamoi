package potamoi.flink.storage

import potamoi.flink.model.{Fcid, FlinkClusterOverview, FlinkJmMetrics, FlinkRestSvcEndpoint, FlinkTmDetail, FlinkTmMetrics, Ftid}
import potamoi.flink.DataStorageErr
import zio.IO
import zio.stream.Stream

/**
 * Flink cluster snapshot storage.
 */
trait ClusterSnapStorage:
  def overview: ClusterOverviewStorage
  def tmDetail: TmDetailStorage
  def jmMetrics: JmMetricsStorage
  def tmMetrics: TmMetricStorage

/**
 * Storage for flink overview, see: [[FlinkClusterOverview]]
 */
trait ClusterOverviewStorage extends ClusterOverviewStorage.Modifier with ClusterOverviewStorage.Query

object ClusterOverviewStorage {
  trait Modifier:
    def put(ov: FlinkClusterOverview): IO[DataStorageErr, Unit]
    def rm(fcid: Fcid): IO[DataStorageErr, Unit]

  trait Query:
    def get(fcid: Fcid): IO[DataStorageErr, Option[FlinkClusterOverview]]
    def listAll: Stream[DataStorageErr, FlinkClusterOverview]
}

/**
 * Storage for flink job-manager metrics.
 */
trait JmMetricsStorage extends JmMetricsStorage.Modify with JmMetricsStorage.Query

object JmMetricsStorage {
  trait Modify:
    def put(metric: FlinkJmMetrics): IO[DataStorageErr, Unit]
    def rm(fcid: Fcid): IO[DataStorageErr, Unit]

  trait Query:
    def get(fcid: Fcid): IO[DataStorageErr, Option[FlinkJmMetrics]]
}

/**
 * Storage for flink task-manager detail, see: [[FlinkTmDetail]].
 */
trait TmDetailStorage extends TmDetailStorage.Modify with TmDetailStorage.Query

object TmDetailStorage {
  trait Modify:
    def put(tm: FlinkTmDetail): IO[DataStorageErr, Unit]
    def rm(fcid: Fcid): IO[DataStorageErr, Unit]
    def rm(ftid: Ftid): IO[DataStorageErr, Unit]

  trait Query:
    def get(ftid: Ftid): IO[DataStorageErr, Option[FlinkTmDetail]]
    def list(fcid: Fcid): IO[DataStorageErr, List[FlinkTmDetail]]
    def listAll: Stream[DataStorageErr, FlinkTmDetail]
    def listTmId(fcid: Fcid): IO[DataStorageErr, List[Ftid]]
    def listAllTmId: Stream[DataStorageErr, Ftid]
}

/**
 * Storage for flink task-manager metrics.
 */
trait TmMetricStorage extends TmMetricStorage.Modify with TmMetricStorage.Query

object TmMetricStorage {
  trait Modify:
    def put(metric: FlinkTmMetrics): IO[DataStorageErr, Unit]
    def rm(ftid: Ftid): IO[DataStorageErr, Unit]
    def rm(fcid: Fcid): IO[DataStorageErr, Unit]

  trait Query:
    def get(ftid: Ftid): IO[DataStorageErr, Option[FlinkTmMetrics]]
    def list(fcid: Fcid): IO[DataStorageErr, List[FlinkTmMetrics]]
}
