package potamoi.flink.storage

import potamoi.flink.DataStoreErr
import potamoi.flink.model.*
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

  def rmSnapData(fcid: Fcid): IO[DataStoreErr, Unit] = {
    overview.rm(fcid) <&>
    tmDetail.rm(fcid) <&>
    jmMetrics.rm(fcid) <&>
    tmMetrics.rm(fcid)
  }

/**
 * Storage for flink overview, see: [[FlinkClusterOverview]]
 */
trait ClusterOverviewStorage extends ClusterOverviewStorage.Modifier with ClusterOverviewStorage.Query

object ClusterOverviewStorage {
  trait Modifier:
    def put(ov: FlinkClusterOverview): IO[DataStoreErr, Unit]
    def rm(fcid: Fcid): IO[DataStoreErr, Unit]

  trait Query:
    def get(fcid: Fcid): IO[DataStoreErr, Option[FlinkClusterOverview]]
    def listAll: Stream[DataStoreErr, FlinkClusterOverview]
}

/**
 * Storage for flink job-manager metrics.
 */
trait JmMetricsStorage extends JmMetricsStorage.Modify with JmMetricsStorage.Query

object JmMetricsStorage {
  trait Modify:
    def put(metric: FlinkJmMetrics): IO[DataStoreErr, Unit]
    def rm(fcid: Fcid): IO[DataStoreErr, Unit]

  trait Query:
    def get(fcid: Fcid): IO[DataStoreErr, Option[FlinkJmMetrics]]
}

/**
 * Storage for flink task-manager detail, see: [[FlinkTmDetail]].
 */
trait TmDetailStorage extends TmDetailStorage.Modify with TmDetailStorage.Query

object TmDetailStorage {
  trait Modify:
    def put(tm: FlinkTmDetail): IO[DataStoreErr, Unit]
    def putAll(tm: List[FlinkTmDetail]): IO[DataStoreErr, Unit]
    def rm(fcid: Fcid): IO[DataStoreErr, Unit]
    def rm(ftid: Ftid): IO[DataStoreErr, Unit]

  trait Query:
    def get(ftid: Ftid): IO[DataStoreErr, Option[FlinkTmDetail]]
    def list(fcid: Fcid): IO[DataStoreErr, List[FlinkTmDetail]]
    def listAll: Stream[DataStoreErr, FlinkTmDetail]
    def listTmId(fcid: Fcid): IO[DataStoreErr, List[Ftid]]
    def listAllTmId: Stream[DataStoreErr, Ftid]
}

/**
 * Storage for flink task-manager metrics.
 */
trait TmMetricStorage extends TmMetricStorage.Modify with TmMetricStorage.Query

object TmMetricStorage {
  trait Modify:
    def put(metric: FlinkTmMetrics): IO[DataStoreErr, Unit]
    def rm(ftid: Ftid): IO[DataStoreErr, Unit]
    def rm(fcid: Fcid): IO[DataStoreErr, Unit]

  trait Query:
    def get(ftid: Ftid): IO[DataStoreErr, Option[FlinkTmMetrics]]
    def list(fcid: Fcid): IO[DataStoreErr, List[FlinkTmMetrics]]
    def listTmId(fcid: Fcid): IO[DataStoreErr, List[Ftid]]
}
