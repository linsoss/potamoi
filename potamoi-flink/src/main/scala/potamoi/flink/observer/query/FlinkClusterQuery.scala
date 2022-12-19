package potamoi.flink.observer.query

import potamoi.flink.model.{Fjid, FlinkSptTriggerStatus}
import potamoi.flink.storage.{ClusterOverviewStorage, JmMetricsStorage, TmDetailStorage, TmMetricStorage}
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


