package potamoi.kubernetes

import com.coralogix.zio.k8s.client.K8sFailure
import potamoi.PotaErr

/**
 * Kubernetes operation error.
 */

sealed trait K8sErr extends PotaErr

object K8sErr:
  case class RequestK8sApiErr(k8sFailure: K8sFailure) extends K8sErr
  case class DirectRequestK8sApiErr(cause: Throwable) extends K8sErr

  sealed trait NotFound                                          extends K8sErr
  case class DeploymentNotFound(name: String, namespace: String) extends NotFound
  case class ServiceNotFound(name: String, namespace: String)    extends NotFound
  case class PodNotFound(name: String, namespace: String)        extends NotFound
  case class ConfigMapNotFound(name: String, namespace: String)  extends NotFound
