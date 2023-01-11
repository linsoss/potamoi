package potamoi.flink.interp.model

import potamoi.codecs
import potamoi.common.ScalaVersion
import potamoi.flink.interp.model.ResultDropStrategies.given
import potamoi.flink.interp.model.ResultDropStrategy.*
import potamoi.flink.model.{FlinkRuntimeMode, FlinkTargetType, InterpSupport}
import potamoi.flink.model.FlinkRuntimeModes.given
import potamoi.flink.model.InterpFlinkTargetTypes.given
import zio.json.JsonCodec

case class SessionDef(
    execType: FlinkTargetType with InterpSupport,
    execMode: FlinkRuntimeMode = FlinkRuntimeMode.Streaming,
    remote: Option[RemoteClusterEndpoint] = None,
    jobName: Option[String] = None,
    jars: List[String] = List.empty,
    sentClusterJars: List[String] = List.empty,
    parallelism: Int = 1,
    extraProps: Map[String, String] = Map.empty,
    resultStore: ResultStoreConf = ResultStoreConf())
    derives JsonCodec

object SessionDef:
  lazy val nonAllowedOverviewConfigKeys = Vector(
    "execution.target",
    "execution.attached",
    "execution.shutdown-on-attached-exit",
    "pipeline.jars"
  )
  def defaultJobName(sessionId: String) = s"potamoi-interp@$sessionId"

case class RemoteClusterEndpoint(
    address: String,
    port: Int = 8081)
    derives JsonCodec

case class ResultStoreConf(
    capacity: Int = 1024,
    dropStrategy: ResultDropStrategy = DropHead)
    derives JsonCodec:
  lazy val limitless: Boolean = capacity < 0

enum ResultDropStrategy:
  case DropHead
  case DropTail

object ResultDropStrategies:
  given JsonCodec[ResultDropStrategy] = codecs.simpleEnumJsonCodec(ResultDropStrategy.values)