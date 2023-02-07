package potamoi.flink.model.deploy

import potamoi.flink.model.deploy.RestartStrategyProp.NonRestart
import potamoi.flink.model.deploy.RestExportType.ClusterIP
import potamoi.flink.model.deploy.RestExportTypes.given
import zio.json.JsonCodec

/**
 * Flink cluster specification properties.
 */
case class ClusterProps(
    cpu: CpuProp = CpuProp(),
    mem: MemProp = MemProp(),
    par: ParProp = ParProp(),
    webui: WebUIProp = WebUIProp(),
    restExportType: RestExportType = ClusterIP,
    restartStg: RestartStrategyProp = NonRestart,
    stateBackend: Option[StateBackendProp] = None,
    jmHa: Option[JmHaProp] = None,
    s3: Option[S3AccessProp] = None,
    udfJars: Set[String] = Set.empty,
    plugins: Set[String] = Set.empty,
    extraProps: Map[String, String] = Map.empty,
    overridePodTemplate: Option[String] = None)
    derives JsonCodec

object ClusterProps:
  lazy val default = ClusterProps()
