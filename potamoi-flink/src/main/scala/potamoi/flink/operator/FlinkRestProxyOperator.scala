package potamoi.flink.operator

import potamoi.flink.model.Fcid
import potamoi.flink.FlinkErr
import zio.IO
import zio.stream.Stream

/**
 * Flink rest endpoint reverse proxy operator.
 */
trait FlinkRestProxyOperator {

  /**
   * Enable proxying the rest server of the target flink cluster to revise service.
   */
  def enable(fcid: Fcid): IO[FlinkErr, Unit]

  /**
   * Disable proxying the rest server of the target flink cluster.
   */
  def disable(fcid: Fcid): IO[FlinkErr, Unit]

  /**
   * Listing the proxying flink cluster.
   */
  def list: Stream[FlinkErr, Fcid]
}
