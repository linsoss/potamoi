package potamoi.flink.model.interact

import potamoi.flink.model.{Fcid, FlinkRuntimeMode, FlinkTargetType, InteractSupport}
import potamoi.flink.model.FlinkRuntimeMode.*
import zio.ZIO

/**
 * Behavior definition configuration for the flink interactive session.
 */
case class InteractSessionDef(
    execType: FlinkTargetType with InteractSupport,
    execMode: FlinkRuntimeMode = Streaming,
    remoteCluster: Option[Fcid] = None,
    jobName: Option[String] = None,
    localJars: List[String] = List.empty,
    clusterJars: List[String] = List.empty,
    parallelism: Int = 1,
    extraProps: Map[String, String] = Map.empty,
    resultStore: ResultStoreConf = ResultStoreConf(),
    allowSinkOperation: Boolean = false)

object InteractSessionDef:

  /**
   * Local target plan.
   * - remoteCluster is unnecessary;
   * - localJars should be equal to clusterJars;
   */
  def local(
      execMode: FlinkRuntimeMode = FlinkRuntimeMode.Streaming,
      jobName: Option[String] = None,
      jars: List[String] = List.empty,
      parallelism: Int = 1,
      extraProps: Map[String, String] = Map.empty,
      resultStore: ResultStoreConf = ResultStoreConf(),
      allowSinkOperation: Boolean = false): InteractSessionDef =
    InteractSessionDef(
      execType = FlinkTargetType.Local,
      execMode = execMode,
      jobName = jobName,
      localJars = jars,
      clusterJars = jars,
      parallelism = parallelism,
      extraProps = extraProps,
      resultStore = resultStore,
      allowSinkOperation = allowSinkOperation
    )

  /**
   * Remote target plan.
   * - remoteCluster is required.
   */
  def remote(
      remoteCluster: Fcid,
      execMode: FlinkRuntimeMode = FlinkRuntimeMode.Streaming,
      jobName: Option[String] = None,
      localJars: List[String] = List.empty,
      clusterJars: List[String] = List.empty,
      parallelism: Int = 1,
      extraProps: Map[String, String] = Map.empty,
      resultStore: ResultStoreConf = ResultStoreConf(),
      allowSinkOperation: Boolean = false): InteractSessionDef =
    InteractSessionDef(
      execType = FlinkTargetType.Remote,
      execMode = execMode,
      remoteCluster = Some(remoteCluster),
      jobName = jobName,
      localJars = localJars,
      clusterJars = clusterJars,
      parallelism = parallelism,
      extraProps = extraProps,
      resultStore = resultStore,
      allowSinkOperation = allowSinkOperation
    )
