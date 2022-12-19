package potamoi.flink.observer.query

import potamoi.flink.model.{Fjid, FlinkSptTriggerStatus}
import potamoi.flink.storage.{JobMetricsStorage, JobOverviewStorage}
import potamoi.flink.FlinkErr
import zio.IO

import scala.concurrent.duration.Duration

/**
 * Flink job observer.
 */
trait FlinkJobQuery {
  def overview: JobOverviewStorage.Query
  def metrics: JobMetricsStorage.Query
  def savepointTrigger: FlinkSavepointTriggerQuery
}

trait FlinkSavepointTriggerQuery {

  /**
   * Get current savepoint trigger status of the flink job.
   */
  def get(fjid: Fjid, triggerId: String): IO[FlinkErr, FlinkSptTriggerStatus]

  /**
   * Watch flink savepoint trigger until it was completed.
   */
  def watch(fjid: Fjid, triggerId: String, timeout: Duration = Duration.Inf): IO[FlinkErr, FlinkSptTriggerStatus]
}
