package potamoi.flink.observer.query

import potamoi.flink.model.{Fjid, FlinkSptTriggerStatus}
import potamoi.flink.storage.*
import potamoi.flink.FlinkErr
import zio.{IO, UIO, ZIO}

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

object FlinkClusterQuery {
  def make(storage: ClusterSnapStorage): UIO[FlinkClusterQuery] = ZIO.succeed(FlinkClusterQueryImpl(storage))
}

/**
 * Default implementation.
 */
class FlinkClusterQueryImpl(storage: ClusterSnapStorage) extends FlinkClusterQuery {
  lazy val overview  = storage.overview
  lazy val tmDetail  = storage.tmDetail
  lazy val jmMetrics = storage.jmMetrics
  lazy val tmMetrics = storage.tmMetrics
}
