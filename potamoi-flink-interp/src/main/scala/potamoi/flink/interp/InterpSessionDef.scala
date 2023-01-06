package potamoi.flink.interp

import potamoi.codecs
import potamoi.common.ScalaVersion
import potamoi.flink.interp.ResultDropStrategy.*
import potamoi.flink.model.{FlinkRuntimeMode, FlinkTargetType, InterpSupport}
import zio.json.JsonCodec
import potamoi.flink.interp.ResultDropStrategies.given
import potamoi.flink.model.InterpFlinkTargetTypes.given
import potamoi.flink.model.FlinkRuntimeModes.given

case class InterpSessionDef(
    execType: FlinkTargetType with InterpSupport,
    execMode: FlinkRuntimeMode = FlinkRuntimeMode.Streaming,
    remote: Option[RemoteClusterEndpoint] = None,
    jobName: Option[String] = None,
    jars: List[String] = List.empty,
    parallelism: Int = 1,
    extraProps: Map[String, String] = Map.empty,
    resultStore: ResultStoreConf = ResultStoreConf())
    derives JsonCodec

object InterpSessionDef:
  lazy val nonAllowedOverviewConfigKeys =
    Vector(
      "execution.target",
      "execution.attached",
      "execution.shutdown-on-attached-exit"
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
