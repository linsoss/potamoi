package potamoi.flink.model.deploy

import potamoi.flink.FlinkVersion
import potamoi.flink.model.*
import zio.json.JsonCodec

/**
 * Flink Kubernetes cluster definition.
 */
sealed trait FlinkClusterDef[SubType <: FlinkClusterDef[SubType]] { this: SubType =>

  val flinkVer: FlinkVersion
  val clusterId: String
  val namespace: String
  val image: String
  val k8sAccount: Option[String]
  val restExportType: RestExportType

  val cpu: CpuConfig
  val mem: MemConfig
  val par: ParConfig
  val webui: WebUIConfig
  val restartStg: RestartStgConfig
  val stateBackend: Option[StateBackendConfig]
  val jmHa: Option[JmHaConfig]
  val s3: Option[S3AccessConf]

  val injectedDeps: Set[String]
  val builtInPlugins: Set[String]
  val extRawConfigs: Map[String, String]
  val overridePodTemplate: Option[String]

  val execType: FlinkTargetType with DeploySupport
  lazy val fcid: Fcid = Fcid(clusterId, namespace)

  private[flink] def copyExtRawConfigs(extRawConfigs: Map[String, String]): SubType
  private[flink] def copyBuiltInPlugins(builtInPlugins: Set[String]): SubType
  private[flink] def copyStateBackend(stateBackend: Option[StateBackendConfig]): SubType
  private[flink] def copyJmHa(jmHa: Option[JmHaConfig]): SubType
  private[flink] def copyInjectDeps(injectedDeps: Set[String]): SubType
}

/**
 * Flink K8s session cluster definition.
 */
case class FlinkSessClusterDef(
    flinkVer: FlinkVersion,
    clusterId: String,
    namespace: String = "default",
    image: String,
    k8sAccount: Option[String] = None,
    restExportType: RestExportType = RestExportType.ClusterIP,
    cpu: CpuConfig = CpuConfig(),
    mem: MemConfig = MemConfig(),
    par: ParConfig = ParConfig(),
    webui: WebUIConfig = WebUIConfig(enableSubmit = true, enableCancel = true),
    restartStg: RestartStgConfig = NonRestartStg,
    stateBackend: Option[StateBackendConfig] = None,
    jmHa: Option[JmHaConfig] = None,
    s3: Option[S3AccessConf] = None,
    injectedDeps: Set[String] = Set.empty,
    builtInPlugins: Set[String] = Set.empty,
    extRawConfigs: Map[String, String] = Map.empty,
    overridePodTemplate: Option[String] = None)
    extends FlinkClusterDef[FlinkSessClusterDef] {

  val execType = FlinkTargetType.K8sSession

  private[flink] def copyExtRawConfigs(extRawConfigs: Map[String, String]): FlinkSessClusterDef      = copy(extRawConfigs = extRawConfigs)
  private[flink] def copyBuiltInPlugins(builtInPlugins: Set[String]): FlinkSessClusterDef            = copy(builtInPlugins = builtInPlugins)
  private[flink] def copyStateBackend(stateBackend: Option[StateBackendConfig]): FlinkSessClusterDef = copy(stateBackend = stateBackend)
  private[flink] def copyJmHa(jmHa: Option[JmHaConfig]): FlinkSessClusterDef                         = copy(jmHa = jmHa)
  private[flink] def copyInjectDeps(injectedDeps: Set[String]): FlinkSessClusterDef                  = copy(injectedDeps = injectedDeps)
}

/**
 * Flink K8s application cluster definition.
 * @param jobJar Flink job jar path, supports local file or s3 path.
 */
case class FlinkAppClusterDef(
    flinkVer: FlinkVersion,
    clusterId: String,
    namespace: String = "default",
    image: String,
    k8sAccount: Option[String] = None,
    restExportType: RestExportType = RestExportType.ClusterIP,
    jobJar: String,
    jobName: Option[String] = None,
    appMain: Option[String] = None,
    appArgs: List[String] = List.empty,
    restore: Option[SavepointRestoreConfig] = None,
    cpu: CpuConfig = CpuConfig(),
    mem: MemConfig = MemConfig(),
    par: ParConfig = ParConfig(),
    webui: WebUIConfig = WebUIConfig(enableSubmit = false, enableCancel = false),
    restartStg: RestartStgConfig = NonRestartStg,
    stateBackend: Option[StateBackendConfig] = None,
    jmHa: Option[JmHaConfig] = None,
    s3: Option[S3AccessConf] = None,
    injectedDeps: Set[String] = Set.empty,
    builtInPlugins: Set[String] = Set.empty,
    extRawConfigs: Map[String, String] = Map.empty,
    overridePodTemplate: Option[String] = None)
    extends FlinkClusterDef[FlinkAppClusterDef] {

  val execType = FlinkTargetType.K8sApplication

  private[flink] def copyExtRawConfigs(extRawConfigs: Map[String, String]): FlinkAppClusterDef      = copy(extRawConfigs = extRawConfigs)
  private[flink] def copyBuiltInPlugins(builtInPlugins: Set[String]): FlinkAppClusterDef            = copy(builtInPlugins = builtInPlugins)
  private[flink] def copyStateBackend(stateBackend: Option[StateBackendConfig]): FlinkAppClusterDef = copy(stateBackend = stateBackend)
  private[flink] def copyJmHa(jmHa: Option[JmHaConfig]): FlinkAppClusterDef                         = copy(jmHa = jmHa)
  private[flink] def copyInjectDeps(injectedDeps: Set[String]): FlinkAppClusterDef                  = copy(injectedDeps = injectedDeps)
}

