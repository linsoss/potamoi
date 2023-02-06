package potamoi.flink.observer.query

import potamoi.flink.{FlinkDataStoreErr, FlinkErr, FlinkRestEndpointRetriever}
import potamoi.flink.model.Fcid
import potamoi.flink.storage.RestEndpointStorage
import potamoi.flink.FlinkErr.K8sFailure
import potamoi.flink.model.snapshot.FlinkRestSvcEndpoint
import potamoi.kubernetes.K8sErr.RequestK8sApiErr
import zio.{IO, UIO, ZIO, ZIOAspect}
import zio.stream.Stream
import zio.ZIO.logDebug
import potamoi.flink.observer.query.FlinkRestEndpointQuery.GetRestEptErr
import potamoi.zios.union

/**
 * Flink kubernetes rest endpoint observer.
 * see [[potamoi.flink.storage.RestEndpointStorage.Query]]
 */
trait FlinkRestEndpointQuery extends RestEndpointStorage.Query {

  /**
   * Retrieve Flink rest endpoint via kubernetes api.
   */
  def retrieve(fcid: Fcid): IO[FlinkErr.K8sFailure, Option[FlinkRestSvcEndpoint]]

  /**
   * Prioritize finding relevant records in [[RestEndpointStorage]], fallback to call k8s api directly
   * when found nothing.
   */
  def getEnsure(fcid: Fcid): IO[GetRestEptErr, Option[FlinkRestSvcEndpoint]]

}

object FlinkRestEndpointQuery {
  type GetRestEptErr = (FlinkErr.K8sFailure | FlinkDataStoreErr) with FlinkErr

  def make(storage: RestEndpointStorage, retriever: FlinkRestEndpointRetriever): UIO[FlinkRestEndpointQuery] =
    ZIO.succeed(FlinkRestEndpointQueryImpl(storage, retriever))
}

/**
 * Default implementation.
 */
class FlinkRestEndpointQueryImpl(storage: RestEndpointStorage, retriever: FlinkRestEndpointRetriever) extends FlinkRestEndpointQuery {

  override def get(fcid: Fcid): IO[FlinkDataStoreErr, Option[FlinkRestSvcEndpoint]]        = storage.get(fcid)
  override def list: Stream[FlinkDataStoreErr, FlinkRestSvcEndpoint]                       = storage.list
  override def retrieve(fcid: Fcid): IO[FlinkErr.K8sFailure, Option[FlinkRestSvcEndpoint]] =
    retriever.retrieve(fcid).mapError(FlinkErr.K8sFailure.apply)

  override def getEnsure(fcid: Fcid): IO[GetRestEptErr, Option[FlinkRestSvcEndpoint]] =
    union[GetRestEptErr, Option[FlinkRestSvcEndpoint]] {
      get(fcid)
        .flatMap {
          case Some(value) => ZIO.succeed(Some(value))
          case None        => retrieve(fcid)
        }
        .catchAll { e =>
          logDebug(s"Fallback to retrieve via k8s api due to: ${e.getMessage}") *>
          retrieve(fcid)
        }
    } @@ ZIOAspect.annotated(fcid.toAnno*)
}
