package potamoi.kubernetes

import com.coralogix.zio.k8s.client.config.httpclient.k8sDefault
import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import com.coralogix.zio.k8s.client.{CodingFailure, DeserializationFailure, K8sFailure, RequestFailure}
import io.circe.Errors
import potamoi.common.SilentErr
import potamoi.syntax.*
import zio.ZIOAppDefault

import scala.util.control.NoStackTrace

/**
 * Kubernetes operation error.
 */
sealed abstract class K8sErr(msg: String, cause: Throwable = SilentErr) extends Exception(msg, cause)

object K8sErr:
  case class DirectRequestK8sApiErr(cause: Throwable)                   extends K8sErr("Request k8s api failure", cause)
  case class RequestK8sApiErr(k8sFailure: K8sFailure, cause: Throwable) extends K8sErr(s"Request k8s api failure: ${k8sFailure.toPrettyStr}", cause)
  case class DeploymentNotFound(name: String, namespace: String)        extends NotFound("Deployment", name, namespace)
  case class ServiceNotFound(name: String, namespace: String)           extends NotFound("Service", name, namespace)
  case class PodNotFound(name: String, namespace: String)               extends NotFound("Pod", name, namespace)
  case class ConfigMapNotFound(name: String, namespace: String)         extends NotFound("ConfigMap", name, namespace)

  sealed abstract class NotFound(rsType: String, name: String, namespace: String)
      extends K8sErr(s"K8s resource not found: $rsType(name=$name, namespace=$namespace}")

  object RequestK8sApiErr:
    def apply(failure: K8sFailure): RequestK8sApiErr = {
      val cause = failure match
        case CodingFailure(_, failure)         => failure
        case RequestFailure(_, reason)         => reason
        case DeserializationFailure(_, errors) => Errors(errors)
        case _                                 => SilentErr
      RequestK8sApiErr(failure, cause)
    }
