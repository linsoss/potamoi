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

  /**
   * Flink cluster overview snapshot query.
   */
  def overview: ClusterOverviewStorage.Query

  /**
   * Flink task-manager snapshot query.
   */
  def tmDetail: TmDetailStorage.Query

  /**
   * Flink job-manager metrics query.
   */
  def jmMetrics: JmMetricsStorage.Query

  /**
   * Flink task-manager metrics query.
   */
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
