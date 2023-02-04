package potamoi.flink.model.interact

import potamoi.{codecs, KryoSerializable}
import potamoi.common.ScalaVersion
import potamoi.flink.model.{FlinkRuntimeMode, FlinkTargetType, InteractSupport}
import potamoi.flink.model.interact.ResultDropStrategies.given
import potamoi.flink.model.interact.ResultDropStrategy.*
import potamoi.flink.model.FlinkRuntimeModes.given
import potamoi.flink.model.InterpFlinkTargetTypes.given
import zio.{durationInt, Duration}
import zio.json.JsonCodec

/**
 * Behavior definition configuration for the flink sql executor.
 */
case class SessionDef(
    execType: FlinkTargetType with InteractSupport,
    execMode: FlinkRuntimeMode = FlinkRuntimeMode.Streaming,
    remoteEndpoint: Option[RemoteClusterEndpoint] = None,
    jobName: Option[String] = None,
    localJars: List[String] = List.empty,
    clusterJars: List[String] = List.empty,
    parallelism: Int = 1,
    extraProps: Map[String, String] = Map.empty,
    resultStore: ResultStoreConf = ResultStoreConf(),
    allowSinkOperation: Boolean = false)
    derives JsonCodec

object SessionDef:

  /**
   * Local target plan.
   * - remoteEndpoint is unnecessary;
   * - localJars should be equal to clusterJars;
   */
  def local(
      execMode: FlinkRuntimeMode = FlinkRuntimeMode.Streaming,
      jobName: Option[String] = None,
      jars: List[String] = List.empty,
      parallelism: Int = 1,
      extraProps: Map[String, String] = Map.empty,
      resultStore: ResultStoreConf = ResultStoreConf(),
      allowSinkOperation: Boolean = false): SessionDef =
    SessionDef(
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
   * - remoteEndpoint is required.
   */
  def remote(
      endpoint: RemoteClusterEndpoint,
      execMode: FlinkRuntimeMode = FlinkRuntimeMode.Streaming,
      jobName: Option[String] = None,
      localJars: List[String] = List.empty,
      clusterJars: List[String] = List.empty,
      parallelism: Int = 1,
      extraProps: Map[String, String] = Map.empty,
      resultStore: ResultStoreConf = ResultStoreConf(),
      allowSinkOperation: Boolean = false): SessionDef =
    SessionDef(
      execType = FlinkTargetType.Remote,
      execMode = execMode,
      remoteEndpoint = Some(endpoint),
      jobName = jobName,
      localJars = localJars,
      clusterJars = clusterJars,
      parallelism = parallelism,
      extraProps = extraProps,
      resultStore = resultStore,
      allowSinkOperation = allowSinkOperation
    )

  lazy val nonAllowedOverviewConfigKeys = Vector(
    "execution.target",
    "execution.attached",
    "execution.shutdown-on-attached-exit",
    "pipeline.jars"
  )
  def defaultJobName(sessionId: String) = s"potamoi-interpreter@$sessionId"

case class RemoteClusterEndpoint(address: String, port: Int) derives JsonCodec

object RemoteClusterEndpoint:
  given Conversion[(String, Int), RemoteClusterEndpoint] = tuple => RemoteClusterEndpoint(tuple._1, tuple._2)

case class ResultStoreConf(capacity: Int = 1024, dropStrategy: ResultDropStrategy = DropHead) derives JsonCodec:
  lazy val limitless: Boolean = capacity < 0

enum ResultDropStrategy:
  case DropHead
  case DropTail

object ResultDropStrategies:
  given JsonCodec[ResultDropStrategy] = codecs.simpleEnumJsonCodec(ResultDropStrategy.values)
