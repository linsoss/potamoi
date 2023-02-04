package potamoi.flink

import com.coralogix.zio.k8s.client.NotFound
import potamoi.flink.FlinkErr
import potamoi.flink.model.{Fcid, FlinkRestSvcEndpoint}
import potamoi.kubernetes.{given_Conversion_String_K8sNamespace, K8sClient, K8sConf, K8sOperatorLive}
import potamoi.kubernetes.K8sErr.RequestK8sApiErr
import zio.{IO, UIO, ZIO, ZLayer}

/**
 * Flink rest endpoint retriever on k8s.
 */
object FlinkRestEndpointRetriever:

  val live: ZLayer[K8sClient, Throwable, FlinkRestEndpointRetriever] = ZLayer.service[K8sClient].project(FlinkRestEndpointRetriever(_))
  def make(k8sClient: K8sClient): UIO[FlinkRestEndpointRetriever]    = ZIO.succeed(FlinkRestEndpointRetriever(k8sClient))

class FlinkRestEndpointRetriever(k8sClient: K8sClient):

  /**
   * Retrieve Flink rest endpoint via kubernetes api.
   */
  def retrieve(fcid: Fcid): IO[RequestK8sApiErr, Option[FlinkRestSvcEndpoint]] =
    k8sClient.services
      .get(s"${fcid.clusterId}-rest", fcid.namespace)
      .flatMap { svc =>
        for {
          metadata  <- svc.getMetadata
          name      <- metadata.getName
          ns        <- metadata.getNamespace
          spec      <- svc.getSpec
          clusterIp <- spec.getClusterIP
          ports     <- spec.getPorts
          restPort   = ports
                         .find(_.port == 8081)
                         .flatMap(_.targetPort.map(_.value.fold(identity, _.toInt)).toOption)
                         .getOrElse(8081)
        } yield Some(FlinkRestSvcEndpoint(name, ns, restPort, clusterIp))
      }
      .catchSome { case NotFound => ZIO.succeed(None) }
      .mapError(RequestK8sApiErr.apply)
