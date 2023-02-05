package potamoi.flink.operator

import potamoi.flink.{FlinkDataStoreErr, FlinkErr}
import potamoi.flink.model.Fcid
import potamoi.flink.storage.FlinkDataStorage
import potamoi.flink.FlinkErr.ClusterIsNotYetTracked
import potamoi.flink.model.snapshot.FlinkRestSvcEndpoint
import zio.{IO, ZIO}
import zio.stream.Stream

/**
 * Flink rest endpoint reverse proxy control.
 *
 * Need to add [[potamoi.flink.FlinkRestReverseProxy#route]] to the http route
 */
trait FlinkRestProxyOperator {

  /**
   * Enable proxying the rest server of the target flink cluster to revise service.
   */
  def enable(fcid: Fcid): IO[(ClusterIsNotYetTracked | FlinkDataStoreErr) with FlinkErr, Unit]

  /**
   * Disable proxying the rest server of the target flink cluster.
   */
  def disable(fcid: Fcid): IO[FlinkDataStoreErr, Unit]

  /**
   * Listing the proxying flink cluster.
   */
  def list: Stream[FlinkDataStoreErr, Fcid]

  /**
   * Listing the proxying flink cluster and the reversed endpoint.
   */
  def listReverseEndpoint: Stream[FlinkDataStoreErr, (Fcid, Option[FlinkRestSvcEndpoint])]
}

/**
 * Default implementation.
 */
class FlinkRestProxyOperatorLive(snapStg: FlinkDataStorage) extends FlinkRestProxyOperator {

  override def enable(fcid: Fcid): IO[(ClusterIsNotYetTracked | FlinkDataStoreErr) with FlinkErr, Unit] =
    snapStg.trackedList.exists(fcid).flatMap {
      case false => ZIO.fail(ClusterIsNotYetTracked(fcid))
      case true  => snapStg.restProxy.put(fcid)
    }

  override def disable(fcid: Fcid): IO[FlinkDataStoreErr, Unit] = snapStg.restProxy.rm(fcid)

  override def list: Stream[FlinkDataStoreErr, Fcid] = snapStg.restProxy.list

  override def listReverseEndpoint: Stream[FlinkDataStoreErr, (Fcid, Option[FlinkRestSvcEndpoint])] =
    snapStg.restProxy.list
      .mapZIO(fcid => snapStg.restEndpoint.get(fcid).map(fcid -> _))
}
