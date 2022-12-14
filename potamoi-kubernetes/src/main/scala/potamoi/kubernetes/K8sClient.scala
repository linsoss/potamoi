package potamoi.kubernetes

import com.coralogix.zio.k8s.client.*
import com.coralogix.zio.k8s.client.apps.v1.deployments.Deployments
import com.coralogix.zio.k8s.client.config.{defaultConfigChain, httpclient, k8sCluster, K8sClusterConfig}
import com.coralogix.zio.k8s.client.config.asynchttpclient.k8sDefault
import com.coralogix.zio.k8s.client.impl.ResourceClient.cluster
import com.coralogix.zio.k8s.client.kubernetes.Kubernetes
import com.coralogix.zio.k8s.client.model.K8sCluster
import com.coralogix.zio.k8s.client.v1.configmaps.ConfigMaps
import com.coralogix.zio.k8s.client.v1.events.Events
import com.coralogix.zio.k8s.client.v1.pods.Pods
import com.coralogix.zio.k8s.client.v1.services.Services
import com.softwaremill.quicklens.modify
import sttp.capabilities.WebSockets
import sttp.capabilities.zio.ZioStreams
import sttp.client3.{basicRequest, Empty, RequestT, SttpBackend}
import sttp.model.Uri
import zio.{Task, ZIO, ZLayer}
import zio.config.syntax.ZIOConfigNarrowOps

/**
 * ZIO-K8s-Client Layer.
 */
object K8sClient:

  val live: ZLayer[K8sConf, Throwable, K8sClient] = {
    val clusterConfig = ZLayer.service[K8sConf].flatMap { conf => defaultConfigChain.project(_.modify(_.client.debug).setTo(conf.get.debug)) }
    val cluster       = clusterConfig >>> k8sCluster
    val client        = clusterConfig >>> httpclient.k8sSttpClient

    (cluster ++ client) >+>
    (Pods.live ++ Services.live ++ Deployments.live ++ ConfigMaps.live ++ Events.live) >>>
    ZLayer {
      for {
        cluster     <- ZIO.service[K8sCluster]
        client      <- ZIO.service[SttpBackend[Task, ZioStreams with WebSockets]]
        pods        <- ZIO.service[Pods.Service]
        services    <- ZIO.service[Services.Service]
        deployments <- ZIO.service[Deployments.Service]
        configmaps  <- ZIO.service[ConfigMaps.Service]
        events      <- ZIO.service[Events.Service]
      } yield K8sClient(cluster, client, pods, services, deployments, configmaps, events)
    }
  }

case class K8sClient(
    private val cluster: K8sCluster,
    private val sttpClient: SttpBackend[Task, ZioStreams with WebSockets],
    pods: Pods.Service,
    services: Services.Service,
    deployments: Deployments.Service,
    configMaps: ConfigMaps.Service,
    events: Events.Service):
  def usingSttp[R, E, A](
      f: (RequestT[Empty, Either[String, String], Any], SttpBackend[Task, ZioStreams with WebSockets], Uri) => ZIO[R, E, A]): ZIO[R, E, A] =
    f(basicRequest, sttpClient, cluster.host)
