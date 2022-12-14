package potamoi.kubernetes

import com.coralogix.zio.k8s.client.{CodingFailure, DeserializationFailure, K8sFailure, RequestFailure}
import com.coralogix.zio.k8s.model.apps.v1.DeploymentSpec
import com.coralogix.zio.k8s.model.core.v1.{PodSpec, ServiceSpec}
import io.circe.Errors
import potamoi.kubernetes.model.PodMetrics
import zio.{IO, UIO, ZIO, ZLayer}
import zio.stream.ZStream

/**
 * Kubernetes operator.
 */
trait K8sOperator {

  def client: UIO[K8sClient]

  /**
   * Get pod metrics info.
   */
  def getPodMetrics(name: String, namespace: String): IO[K8sErr, PodMetrics]

  /**
   * Get deployment spec.
   */
  def getDeploymentSpec(name: String, namespace: String): IO[K8sErr, DeploymentSpec]

  /**
   * Get service spec.
   */
  def getServiceSpec(name: String, namespace: String): IO[K8sErr, ServiceSpec]

  /**
   * Get pod spec.
   */
  def getPodSpec(name: String, namespace: String): IO[K8sErr, PodSpec]

  /**
   * Get configmaps data.
   */
  def getConfigMapsData(name: String, namespace: String): IO[K8sErr, Map[String, String]]

  /**
   * Get pod logging.
   * see kubernetes api: GET /api/v1/namespaces/{namespace}/pods/{name}/log
   */
  def getPodLog(
      podName: String,
      namespace: String,
      containerName: Option[String] = None,
      follow: Boolean = false,
      tailLines: Option[Int] = None,
      sinceSec: Option[Int] = None): ZStream[Any, K8sErr, String]
}

object K8sOperator {

  lazy val live: ZLayer[K8sConf, Throwable, K8sOperatorLive] = (ZLayer.service[K8sConf] >>> K8sClient.live).project(K8sOperatorLive(_))

  def client: ZIO[K8sOperator, Nothing, K8sClient] =
    ZIO.serviceWithZIO[K8sOperator](_.client)

  def getPodMetrics(name: String, namespace: String): ZIO[K8sOperator, K8sErr, PodMetrics] =
    ZIO.serviceWithZIO[K8sOperator](_.getPodMetrics(name, namespace))

  def getDeploymentSpec(name: String, namespace: String): ZIO[K8sOperator, K8sErr, DeploymentSpec] =
    ZIO.serviceWithZIO[K8sOperator](_.getDeploymentSpec(name, namespace))

  def getServiceSpec(name: String, namespace: String): ZIO[K8sOperator, K8sErr, ServiceSpec] =
    ZIO.serviceWithZIO[K8sOperator](_.getServiceSpec(name, namespace))

  def getPodSpec(name: String, namespace: String): ZIO[K8sOperator, K8sErr, PodSpec] =
    ZIO.serviceWithZIO[K8sOperator](_.getPodSpec(name, namespace))

  def getConfigMapsData(name: String, namespace: String): ZIO[K8sOperator, K8sErr, Map[String, String]] =
    ZIO.serviceWithZIO[K8sOperator](_.getConfigMapsData(name, namespace))

  def getPodLog(
      podName: String,
      namespace: String,
      containerName: Option[String],
      follow: Boolean = false,
      tailLines: Option[Int],
      sinceSec: Option[Int]): ZStream[K8sOperator, K8sErr, String] =
    ZStream.unwrap(ZIO.serviceWith[K8sOperator](_.getPodLog(podName, namespace, containerName, follow, tailLines, sinceSec)))
}
