package potamoi.sharding

import com.coralogix.zio.k8s.client.model.K8sNamespace
import com.coralogix.zio.k8s.client.v1.pods.Pods
import com.devsisters.shardcake.{K8sConfig, K8sPodsHealth}
import com.devsisters.shardcake.interfaces.PodsHealth
import potamoi.kubernetes.{K8sClient, K8sConf}
import zio.ZLayer

/**
 * Shardcake k8s pod health check layer.
 */
object KubernetesPodsHealth:

  val live: ZLayer[K8sConf, Throwable, PodsHealth] = {
    val k8sConf   = ZLayer.service[K8sConf]
    val pods      = (k8sConf >>> K8sClient.live).project(_.pods)
    val k8sConfig = k8sConf.project(conf => K8sConfig.default.copy(namespace = conf.namespace.map(K8sNamespace(_))))
    pods ++ k8sConfig >>> K8sPodsHealth.live
  }
