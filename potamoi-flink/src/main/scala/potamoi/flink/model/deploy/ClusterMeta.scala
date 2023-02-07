package potamoi.flink.model.deploy

import potamoi.flink.FlinkVersion
import potamoi.flink.model.Fcid
import zio.json.JsonCodec

/**
 * Flink cluster specification meta info.
 */
case class ClusterMeta(
    flinkVer: FlinkVersion,
    clusterId: String,
    namespace: String = "default",
    image: String,
    k8sAccount: Option[String] = None)
    derives JsonCodec:
  val fcid: Fcid = Fcid(clusterId, namespace)
