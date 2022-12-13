package potamoi.flink.model

import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}

/**
 * Flink job execution mode.
 */
enum FlinkExecMode(val value: String):
  case K8sApplication extends FlinkExecMode("kubernetes-application")
  case K8sSession     extends FlinkExecMode("kubernetes-session")
  case Unknown        extends FlinkExecMode("unknown")

object FlinkExecModes:
  given JsonCodec[FlinkExecMode] = JsonCodec(
    JsonEncoder[String].contramap(_.value),
    JsonDecoder[String].map(s => FlinkExecMode.values.find(_.value == s).getOrElse(FlinkExecMode.Unknown))
  )

  /**
   * infer execution mode from flink raw config value of "execution.target"
   */
  import FlinkExecMode.*
  def ofRawConfValue(executionTarget: Option[String]): FlinkExecMode = executionTarget match {
    case Some("kubernetes-session") => K8sSession
    case Some("remote")             => K8sSession
    case Some("embedded")           => K8sApplication
    case _                          => Unknown
  }
