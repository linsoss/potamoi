package potamoi.kubernetes

import com.coralogix.zio.k8s.client.{NotFound, *}
import com.coralogix.zio.k8s.model.apps.v1.DeploymentSpec
import com.coralogix.zio.k8s.model.core.v1.{PodSpec, ServiceSpec}
import io.circe.Errors
import org.joda.time.DateTime
import potamoi.common.SttpExtension
import potamoi.kubernetes.model.{ContainerMetrics, K8sQuantity, PodMetrics}
import potamoi.kubernetes.K8sErr.*
import potamoi.kubernetes.given_Conversion_String_K8sNamespace
import potamoi.sttps.*
import sttp.client3.*
import zio.{IO, UIO, ZIO, ZLayer}
import zio.ZIO.attempt
import zio.prelude.data.Optional.{Absent, Present}
import zio.stream.ZStream
import zio.Console.printLine

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

  lazy val clive: ZLayer[K8sClient, Throwable, K8sOperator] = ZLayer.service[K8sClient].project(K8sOperatorLive(_))
  lazy val live: ZLayer[K8sConf, Throwable, K8sOperator]    = ZLayer.service[K8sConf] >>> K8sClient.live >>> clive

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

/**
 * Implementation based on ZIO-K8s.
 */
class K8sOperatorLive(k8sClient: K8sClient) extends K8sOperator {

  override def client: UIO[K8sClient] = ZIO.succeed(k8sClient)

  override def getPodMetrics(name: String, namespace: String): IO[PodNotFound | DirectRequestK8sApiErr, PodMetrics] =
    k8sClient.usingSttp { (request, backend, host) =>
      request
        .get(uri"$host/apis/metrics.k8s.io/v1beta1/namespaces/$namespace/pods/$name")
        .send(backend)
        .flattenBody
        .attemptBody { rsp =>
          val json = ujson.read(rsp)
          val ts   = DateTime.parse(json("timestamp").str).getMillis
          val containers = json("containers").arr.map { container =>
            val name = container("name").str
            val cpu  = K8sQuantity(container("usage").obj("cpu").str)
            val mem  = K8sQuantity(container("usage").obj("memory").str)
            ContainerMetrics(name, cpu, mem)
          }
          PodMetrics(ts, containers.toVector)
        }
        .mapError {
          case SttpExtension.NotFound => PodNotFound(name, namespace)
          case e                      => DirectRequestK8sApiErr(e)
        }
    }

  override def getDeploymentSpec(name: String, namespace: String): IO[DeploymentNotFound | RequestK8sApiErr, DeploymentSpec] =
    k8sClient.deployments
      .get(name, namespace)
      .flatMap(_.getSpec)
      .mapError {
        case NotFound => DeploymentNotFound(name, namespace)
        case e        => RequestK8sApiErr(e)
      }

  override def getServiceSpec(name: String, namespace: String): IO[ServiceNotFound | RequestK8sApiErr, ServiceSpec] =
    k8sClient.services
      .get(name, namespace)
      .flatMap(_.getSpec)
      .mapError {
        case NotFound => ServiceNotFound(name, namespace)
        case e        => RequestK8sApiErr(e)
      }

  override def getPodSpec(name: String, namespace: String): IO[PodNotFound | RequestK8sApiErr, PodSpec] =
    k8sClient.pods
      .get(name, namespace)
      .flatMap(_.getSpec)
      .mapError {
        case NotFound => PodNotFound(name, namespace)
        case e        => RequestK8sApiErr(e)
      }

  override def getConfigMapsData(name: String, namespace: String): IO[ConfigMapNotFound | RequestK8sApiErr, Map[String, String]] =
    k8sClient.configMaps
      .get(name, namespace)
      .mapBoth(
        {
          case NotFound => ConfigMapNotFound(name, namespace)
          case e        => RequestK8sApiErr(e)
        }, {
          _.data match {
            case Present(map) => map
            case Absent       => Map.empty[String, String]
          }
        })

  override def getPodLog(
      podName: String,
      namespace: String,
      containerName: Option[String],
      follow: Boolean,
      tailLines: Option[Int],
      sinceSec: Option[Int]): ZStream[Any, PodNotFound | RequestK8sApiErr, String] =
    k8sClient.pods
      .getLog(
        name = podName,
        namespace = namespace,
        container = containerName,
        tailLines = tailLines,
        sinceSeconds = sinceSec,
        follow = Some(follow),
        insecureSkipTLSVerifyBackend = Some(true)
      )
      .mapError {
        case NotFound => PodNotFound(podName, namespace)
        case e        => RequestK8sApiErr(e)
      }
}
