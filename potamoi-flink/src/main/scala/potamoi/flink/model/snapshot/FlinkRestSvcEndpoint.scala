package potamoi.flink.model.snapshot

import potamoi.flink.{FlinkConf, FlinkRestEndpointType}
import potamoi.flink.FlinkRestEndpointType.*
import potamoi.KryoSerializable
import potamoi.flink.model.snapshot.FlinkK8sServiceSnap
import zio.json.JsonCodec

/**
 * K8s svc endpoint of flink rest-service.
 */
case class FlinkRestSvcEndpoint(svcName: String, svcNs: String, port: Int, clusterIp: String) extends KryoSerializable derives JsonCodec {

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
    case ClusterIP => clusterIpRest

  /**
   * Choose host type by [[FlinkRestEndpointType]].
   */
  def chooseHost(using endpointType: FlinkRestEndpointType): String = endpointType match
    case SvcDns    => dns
    case ClusterIP => clusterIp

  def show: String = s"svcName=$svcName, svcNs=$svcNs, port=$port, clusterIP=$clusterIp"
}

object FlinkRestSvcEndpoint:

  def of(svcSnap: FlinkK8sServiceSnap): Option[FlinkRestSvcEndpoint] = {
    if (!svcSnap.isFlinkRestSvc) None
    else
      for {
        clusterIp <- svcSnap.clusterIP
        port      <- svcSnap.ports.find(_.name == "rest").map(_.port)
        name       = svcSnap.name
        ns         = svcSnap.namespace
      } yield FlinkRestSvcEndpoint(name, ns, port, clusterIp)
  }
