package potamoi.flink.storage

import potamoi.flink.FlinkDataStoreErr
import potamoi.flink.model.*
import potamoi.flink.model.snapshot.{FlinkClusterOverview, FlinkJmMetrics, FlinkTmDetail, FlinkTmMetrics}
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

  def rmSnapData(fcid: Fcid): IO[FlinkDataStoreErr, Unit] = {
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
    def put(ov: FlinkClusterOverview): IO[FlinkDataStoreErr, Unit]
    def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]

  trait Query:
    def get(fcid: Fcid): IO[FlinkDataStoreErr, Option[FlinkClusterOverview]]
    def listAll: Stream[FlinkDataStoreErr, FlinkClusterOverview]
}

/**
 * Storage for flink job-manager metrics.
 */
trait JmMetricsStorage extends JmMetricsStorage.Modify with JmMetricsStorage.Query

object JmMetricsStorage {
  trait Modify:
    def put(metric: FlinkJmMetrics): IO[FlinkDataStoreErr, Unit]
    def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]

  trait Query:
    def get(fcid: Fcid): IO[FlinkDataStoreErr, Option[FlinkJmMetrics]]
}

/**
 * Storage for flink task-manager detail, see: [[FlinkTmDetail]].
 */
trait TmDetailStorage extends TmDetailStorage.Modify with TmDetailStorage.Query

object TmDetailStorage {
  trait Modify:
    def put(tm: FlinkTmDetail): IO[FlinkDataStoreErr, Unit]
    def putAll(tm: List[FlinkTmDetail]): IO[FlinkDataStoreErr, Unit]
    def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]
    def rm(ftid: Ftid): IO[FlinkDataStoreErr, Unit]

  trait Query:
    def get(ftid: Ftid): IO[FlinkDataStoreErr, Option[FlinkTmDetail]]
    def list(fcid: Fcid): IO[FlinkDataStoreErr, List[FlinkTmDetail]]
    def listAll: Stream[FlinkDataStoreErr, FlinkTmDetail]
    def listTmId(fcid: Fcid): IO[FlinkDataStoreErr, List[Ftid]]
    def listAllTmId: Stream[FlinkDataStoreErr, Ftid]
}

/**
 * Storage for flink task-manager metrics.
 */
trait TmMetricStorage extends TmMetricStorage.Modify with TmMetricStorage.Query

object TmMetricStorage {
  trait Modify:
    def put(metric: FlinkTmMetrics): IO[FlinkDataStoreErr, Unit]
    def rm(ftid: Ftid): IO[FlinkDataStoreErr, Unit]
    def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]

  trait Query:
    def get(ftid: Ftid): IO[FlinkDataStoreErr, Option[FlinkTmMetrics]]
    def list(fcid: Fcid): IO[FlinkDataStoreErr, List[FlinkTmMetrics]]
    def listTmId(fcid: Fcid): IO[FlinkDataStoreErr, List[Ftid]]
}
