package potamoi.flink.observer.query

import potamoi.flink.model.{Fjid, FlinkSptTriggerStatus}
import potamoi.flink.storage.{ClusterOverviewStorage, ClusterSnapStorage, FlinkSnapshotStorage, JmMetricsStorage, TmDetailStorage, TmMetricStorage}
import potamoi.flink.FlinkErr
import zio.IO

import scala.concurrent.duration.Duration

/**
 * Flink cluster observer.
 */
trait FlinkClusterQuery {
  def overview: ClusterOverviewStorage.Query
  def tmDetail: TmDetailStorage.Query
  def jmMetrics: JmMetricsStorage.Query
  def tmMetrics: TmMetricStorage.Query
}

/**
 * Default implementation.
 */
case class FlinkClusterQueryLive(storage: ClusterSnapStorage) extends FlinkClusterQuery {
  lazy val overview  = storage.overview
  lazy val tmDetail  = storage.tmDetail
  lazy val jmMetrics = storage.jmMetrics
  lazy val tmMetrics = storage.tmMetrics
}
