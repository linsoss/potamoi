package potamoi.kubernetes
import com.coralogix.zio.k8s.client.NotFound
import com.coralogix.zio.k8s.model.apps.v1.DeploymentSpec
import com.coralogix.zio.k8s.model.core.v1.{PodSpec, ServiceSpec}
import org.joda.time.DateTime
import potamoi.kubernetes.model.{ContainerMetrics, K8sQuantity, PodMetrics}
import zio.{IO, UIO, ZIO}
import zio.ZIO.attempt
import potamoi.sttps.*
import sttp.client3.*
import zio.prelude.data.Optional.{Absent, Present}
import zio.stream.ZStream
import K8sErr.*
import potamoi.kubernetes.given_Conversion_String_K8sNamespace
import zio.Console.printLine

class K8sOperatorLive(k8sClient: K8sClient) extends K8sOperator {

  override def client: UIO[K8sClient] = ZIO.succeed(k8sClient)

  override def getPodMetrics(name: String, namespace: String): IO[DirectRequestK8sApiErr, PodMetrics] =
    k8sClient.usingSttp { (request, backend, host) =>
      request
        .get(uri"$host/apis/metrics.k8s.io/v1beta1/namespaces/$namespace/pods/$name")
        .send(backend)
        .map(_.body)
        .narrowEither
        .flatMap { rsp =>
          attempt {
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
        }
        .mapError(e => DirectRequestK8sApiErr(e))
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
