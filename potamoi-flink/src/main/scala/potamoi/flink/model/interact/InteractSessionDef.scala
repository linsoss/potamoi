package potamoi.flink.model.interact

import potamoi.flink.model.{Fcid, FlinkRuntimeMode, FlinkTargetType, InteractSupport}
import potamoi.flink.model.FlinkRuntimeMode.*
import potamoi.flink.model.interact.InteractSessionDef.*
import potamoi.flink.model.interact.ResultDropStrategy.DropHead
import zio.{durationInt, Duration, ZIO}

import scala.annotation.unused

/**
 * Behavior definition configuration for the flink interactive session.
 */
case class InteractSessionDef(
    execType: FlinkTargetType with InteractSupport,
    execMode: FlinkRuntimeMode = defaultExecMode,
    remoteCluster: Option[Fcid] = None,
    jobName: Option[String] = None,
    localJars: List[String] = List.empty,
    clusterJars: List[String] = List.empty,
    parallelism: Int = defaultParallelism,
    extraProps: Map[String, String] = Map.empty,
    resultStore: ResultStoreConf = defaultResultStore,
    allowSinkOperation: Boolean = defaultAllowSinkOperation)

object InteractSessionDef:

  val defaultExecMode: FlinkRuntimeMode   = Streaming
  val defaultParallelism: Int             = 1
  val defaultAllowSinkOperation: Boolean  = false
  val defaultResultStore: ResultStoreConf = ResultStoreConf(1024, DropHead)

  /**
   * Local target plan.
   * - remoteCluster is unnecessary;
   * - localJars should be equal to clusterJars;
   */
  def local(
      execMode: FlinkRuntimeMode = defaultExecMode,
      jobName: Option[String] = None,
      jars: List[String] = List.empty,
      parallelism: Int = defaultParallelism,
      extraProps: Map[String, String] = Map.empty,
      resultStore: ResultStoreConf = defaultResultStore,
      allowSinkOperation: Boolean = defaultAllowSinkOperation): InteractSessionDef =
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
      execMode: FlinkRuntimeMode = defaultExecMode,
      jobName: Option[String] = None,
      localJars: List[String] = List.empty,
      clusterJars: List[String] = List.empty,
      parallelism: Int = defaultParallelism,
      extraProps: Map[String, String] = Map.empty,
      resultStore: ResultStoreConf = defaultResultStore,
      allowSinkOperation: Boolean = defaultAllowSinkOperation): InteractSessionDef =
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
