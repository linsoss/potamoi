package potamoi.flink.model

import potamoi.codecs
import potamoi.flink.model.FlinkTargetType.*
import zio.json.JsonCodec

/**
 * Flink execution target.
 *
 * https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/config/#execution-target
 */
enum FlinkTargetType(val rawValue: String):
  case Local                                extends FlinkTargetType("local") with InterpSupport
  case Remote                               extends FlinkTargetType("remote") with InterpSupport
  case Embedded                             extends FlinkTargetType("embedded")
  case K8sSession                           extends FlinkTargetType("kubernetes-session") with DeploySupport
  case K8sApplication                       extends FlinkTargetType("kubernetes-application") with DeploySupport
  case Other(override val rawValue: String) extends FlinkTargetType(rawValue)

object FlinkTargetTypes:
  lazy val values: Array[FlinkTargetType] = Array(Local, Remote, Embedded, K8sSession, K8sApplication)

  given JsonCodec[FlinkTargetType] =
    codecs.stringBasedJsonCodec(
      decode = s => values.find(_.toString == s).orElse(Some(Other(s))),
      encode = _ match
        case Other(rawValue) => rawValue
        case mode            => mode.toString
    )

  def ofRawValue(raw: String): FlinkTargetType =
    values.find(_.rawValue == raw).getOrElse(FlinkTargetType.Other(raw))

/**
 * Flink target types of interactive execution supported.
 */
sealed trait InterpSupport

object InterpFlinkTargetTypes:
  lazy val values: Array[FlinkTargetType with InterpSupport] = Array(Local, Remote)
  given JsonCodec[FlinkTargetType with InterpSupport]        = codecs.simpleEnumJsonCodec(values)

/**
 * Flink target types of cluster deployment supported.
 */
sealed trait DeploySupport

object DeployFlinkTargetTypes:
  lazy val values: Array[FlinkTargetType with DeploySupport] = Array(K8sSession, K8sApplication)
  given JsonCodec[FlinkTargetType with DeploySupport]        = codecs.simpleEnumJsonCodec(values)
