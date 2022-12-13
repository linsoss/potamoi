package potamoi.flink.observer

import potamoi.flink.model.{Fjid, FlinkSptTriggerStatus}
import potamoi.flink.FlinkErr
import zio.IO

import scala.concurrent.duration.Duration

/**
 * Flink Savepoint trigger observer.
 */
trait SavepointTriggerQuery {

  /**
   * Get current savepoint trigger status of the flink job.
   */
  def get(fjid: Fjid, triggerId: String): IO[FlinkErr, FlinkSptTriggerStatus]

  /**
   * Watch flink savepoint trigger until it was completed.
   */
  def watch(fjid: Fjid, triggerId: String, timeout: Duration = Duration.Inf): IO[FlinkErr, FlinkSptTriggerStatus]
  
}
