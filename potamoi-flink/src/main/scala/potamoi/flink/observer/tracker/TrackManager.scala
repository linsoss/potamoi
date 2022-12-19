package potamoi.flink.observer.tracker

import potamoi.flink.{DataStorageErr, FlinkErr}
import potamoi.flink.model.Fcid
import zio.{IO, Task}
import zio.stream.Stream

/**
 * Flink cluster trackers manager.
 */
trait TrackManager {

  /**
   * Tracking flink cluster.
   */
  def track(fcid: Fcid): IO[FlinkErr, Unit]

  /**
   * UnTracking flink cluster.
   */
  def untrack(fcid: Fcid): IO[FlinkErr, Unit]

  /**
   * Whether the tracked fcid exists.
   */
  def isBeTracked(fcid: Fcid): IO[FlinkErr, Boolean]

  /**
   * Listing tracked cluster id.
   */
  def listTrackedClusters: Stream[FlinkErr, Fcid]

}
