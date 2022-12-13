package potamoi.flink.observer

import potamoi.flink.model.{Fcid, FlinkRestSvcEndpoint}
import potamoi.flink.FlinkErr
import zio.IO
import zio.stream.Stream

/**
 * Flink cluster rest endpoint snapshot query layer.
 */
trait RestEndpointQuery {

  /**
   * Get Flink rest endpoint via kubernetes api.
   * Prioritize finding relevant records in cache, and call k8s api directly as fallback
   * when found nothing.
   *
   * @param directly retrieve the endpoint via kubernetes api directly and reset the cache immediately.
   */
  def get(fcid: Fcid, directly: Boolean = false): IO[FlinkErr, FlinkRestSvcEndpoint]

  /**
   * Similar to [[get]], but returns an None value instead of the [[FlinkErr.ClusterNotFound]] error.
   */
  def retrieve(fcid: Fcid, directly: Boolean = false): IO[FlinkErr, Option[FlinkRestSvcEndpoint]]

  /**
   * List all Flink rest endpoint that in tracked.
   */
  def list: Stream[FlinkErr, (Fcid, FlinkRestSvcEndpoint)]

}
