package potamoi.flink.observer.query

import potamoi.flink.model.{Fcid, FlinkRestSvcEndpoint}
import potamoi.flink.DataStorageErr
import potamoi.flink.storage.RestEndpointStorage
import potamoi.kubernetes.K8sErr.RequestK8sApiErr
import zio.IO
import zio.stream.Stream

/**
 * Flink kubernetes rest endpoint observer.
 * see [[potamoi.flink.storage.RestEndpointStorage.Query]]
 */
trait FlinkRestEndpointQuery extends RestEndpointStorage.Query {

  /**
   * Retrieve Flink rest endpoint via kubernetes api.
   */
  def retrieve(fcid: Fcid): IO[RequestK8sApiErr, Option[FlinkRestSvcEndpoint]]
}
