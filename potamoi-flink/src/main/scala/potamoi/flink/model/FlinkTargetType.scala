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
  case Local           extends FlinkTargetType("local") with InteractSupport
  case Remote          extends FlinkTargetType("remote") with InteractSupport
  case Embedded        extends FlinkTargetType("embedded")
  case K8sSession      extends FlinkTargetType("kubernetes-session") with DeploySupport
  case K8sApplication  extends FlinkTargetType("kubernetes-application") with DeploySupport
  case YarnSession     extends FlinkTargetType("yarn-session")
  case YarnApplication extends FlinkTargetType("yarn-application")
  case YarnPerJob      extends FlinkTargetType("yarn-per-job")
  case Unknown         extends FlinkTargetType("unknown")

object FlinkTargetTypes:
  given JsonCodec[FlinkTargetType]             = codecs.simpleEnumJsonCodec(FlinkTargetType.values)
  lazy val effectValues                        = Set(Local, Remote, Embedded, K8sSession, K8sApplication, YarnSession, YarnApplication, YarnPerJob)
  def ofRawValue(raw: String): FlinkTargetType = values.find(_.rawValue == raw).getOrElse(Unknown)

/**
 * Flink target types of interactive execution supported.
 */
sealed trait InteractSupport

object InterpFlinkTargetTypes:
  lazy val values: Array[FlinkTargetType with InteractSupport] = Array(Local, Remote)
  given JsonCodec[FlinkTargetType with InteractSupport]        = codecs.simpleEnumJsonCodec(values)

/**
 * Flink target types of cluster deployment supported.
 */
sealed trait DeploySupport

object DeployFlinkTargetTypes:
  lazy val values: Array[FlinkTargetType with DeploySupport] = Array(K8sSession, K8sApplication)
  given JsonCodec[FlinkTargetType with DeploySupport]        = codecs.simpleEnumJsonCodec(values)
