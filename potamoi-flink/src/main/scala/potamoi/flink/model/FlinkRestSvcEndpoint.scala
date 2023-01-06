package potamoi.flink.model

import potamoi.flink.{FlinkConf, FlinkRestEndpointType}
import potamoi.flink.FlinkRestEndpointType.*
import zio.json.JsonCodec

/**
 * K8s svc endpoint of flink rest-service.
 */
case class FlinkRestSvcEndpoint(svcName: String, svcNs: String, port: Int, clusterIp: String) derives JsonCodec {

  /**
   * DNS name of the k8s svc.
   */
  lazy val dns = s"$svcName.$svcNs"

  /**
   * DNS url of the k8s svc.
   */
  lazy val dnsRest = s"http://$dns:$port"

  /**
   * Cluster-IP url of the k8s svc.
   */
  lazy val clusterIpRest = s"http://$clusterIp:$port"

  /**
   * Choose rest url type by [[FlinkRestEndpointType]].
   */
  def chooseUrl(using endpointType: FlinkRestEndpointType): String = endpointType match
    case SvcDns    => dnsRest
    case ClusterIp => clusterIpRest

  /**
   * Choose host type by [[FlinkRestEndpointType]].
   */
  def chooseHost(using endpointType: FlinkRestEndpointType): String = endpointType match
    case SvcDns    => dns
    case ClusterIp => clusterIp

  def show: String = s"svcName=$svcName, svcNs=$svcNs, port=$port, clusterIP=$clusterIp"
}

object FlinkRestSvcEndpoint:

  def of(svcSnap: FlinkK8sServiceSnap): Option[FlinkRestSvcEndpoint] = {
    if (!svcSnap.isFlinkRestSvc) None
    else
      for {
        clusterIp <- svcSnap.clusterIP
        port      <- svcSnap.ports.find(_.name == "rest").map(_.port)
        name = svcSnap.name
        ns   = svcSnap.namespace
      } yield FlinkRestSvcEndpoint(name, ns, port, clusterIp)
  }
