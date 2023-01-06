package potamoi.flink.interp

import potamoi.common.ScalaVersion
import potamoi.flink.interp.ResultDropStrategy.*
import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}

case class SessionDef(
    mode: SessExecMode,
    rtMode: RuntimeMode,
    remote: Option[RemoteClusterEndpoint] = None,
    jobName: Option[String] = None,
    jars: List[String] = List.empty,
    parallelism: Int = 1,
    extraProps: Map[String, String] = Map.empty,
    resultStore: ResultStoreConf = ResultStoreConf())

enum SessExecMode(rawTarget: String):
  case Local  extends SessExecMode("local")
  case Remote extends SessExecMode("remote")

enum RuntimeMode:

case class RemoteClusterEndpoint(address: String, port: Int = 8081)

case class ResultStoreConf(capacity: Int = 1024, dropStrategy: ResultDropStrategy = DropHead):
  lazy val limitless: Boolean = capacity < 0

enum ResultDropStrategy:
  case DropHead
  case DropTail
