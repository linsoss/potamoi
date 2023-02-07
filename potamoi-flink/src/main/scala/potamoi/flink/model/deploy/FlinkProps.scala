package potamoi.flink.model.deploy

import potamoi.codecs
import potamoi.flink.model.deploy.CheckpointStorageTypes.given
import potamoi.flink.model.deploy.FlinkProps.resolve
import potamoi.flink.model.deploy.RestExportTypes.given
import potamoi.flink.model.deploy.SavepointRestoreModes.given
import potamoi.flink.model.deploy.StateBackendTypes.given
import potamoi.nums.*
import potamoi.syntax.contra
import zio.json.{jsonField, DeriveJsonCodec, JsonCodec}

/**
 * Type-safe flink major configuration entries.
 */
sealed trait FlinkProps {

  /**
   * Mapping to raw flink configuration items.
   */
  def mapping: Map[String, String]
}

object FlinkProps {

  /**
   * Eliminate empty configuration items and convert to String format.
   */
  extension (items: Map[String, _])
    protected[deploy] inline def resolve: Map[String, String] = items
      // filter empty value
      .filter { case (_, value) =>
        value match
          case None                     => false
          case value: Iterable[_]       => value.nonEmpty
          case Some(value: Iterable[_]) => value.nonEmpty
          case _                        => true
      }
      // flatten option value.
      .map {
        case (key, Some(value)) => key -> value
        case (k, v)             => k   -> v
      }
      // convert value type to string.
      .map {
        case (k, value: Map[_, _])   => k -> value.map(kv => s"${kv._1}:${kv._2}").mkString(",")
        case (k, value: Iterable[_]) => k -> value.mkString(",")
        case (k, v)                  => k -> v.toString
      }
}

/**
 * Flink k8s cpu configuration.
 */
case class CpuProp(
    jm: Double = 1.0,
    tm: Double = -1.0,
    jmFactor: Double = 1.0,
    tmFactor: Double = 1.0)
    extends FlinkProps
    derives JsonCodec {

  def mapping: Map[String, String] = Map(
    "kubernetes.taskmanager.cpu"              -> jm.ensureDoubleOr(_ > 0, 1.0),
    "kubernetes.jobmanager.cpu.limit-factor"  -> jmFactor.ensureDoubleOr(_ > 0, 1.0),
    "kubernetes.taskmanager.cpu"              -> tm,
    "kubernetes.taskmanager.cpu.limit-factor" -> tmFactor.ensureDoubleOr(_ > 0, 1.0)
  ).resolve
}

/**
 * Flink memory configuration.
 */
case class MemProp(
    jmMB: Int = 1920,
    tmMB: Int = 1920)
    extends FlinkProps
    derives JsonCodec {

  def mapping: Map[String, String] = Map(
    "jobmanager.memory.process.size"  -> jmMB.ensureIntOr(_ > 0, 1920).contra(_ + "m"),
    "taskmanager.memory.process.size" -> tmMB.ensureIntOr(_ > 0, 1920).contra(_ + "m")
  ).resolve
}

/**
 * Flink parallelism configuration.
 */
case class ParProp(
    numOfSlot: Int = 1,
    parDefault: Int = 1)
    extends FlinkProps
    derives JsonCodec {

  def mapping: Map[String, String] = Map(
    "taskmanager.numberOfTaskSlots" -> numOfSlot.ensureIntMin(1),
    "parallelism.default"           -> parDefault.ensureIntMin(1)
  ).resolve
}

/**
 * Flink web ui service configuration.
 */
case class WebUIProp(
    enableSubmit: Boolean = false,
    enableCancel: Boolean = true)
    extends FlinkProps
    derives JsonCodec {

  def mapping: Map[String, String] = Map(
    "web.submit.enable" -> enableSubmit,
    "web.cancel.enable" -> enableCancel
  ).resolve
}

/**
 * Flink rest endpoint export type.
 */
enum RestExportType(val rawValue: String):
  case ClusterIP         extends RestExportType("ClusterIP")
  case NodePort          extends RestExportType("NodePort")
  case LoadBalancer      extends RestExportType("LoadBalancer")
  case HeadlessClusterIP extends RestExportType("Headless_ClusterIP")

object RestExportTypes:
  given JsonCodec[RestExportType] = codecs.simpleEnumJsonCodec(RestExportType.values)

/**
 * Flink task restart strategy.
 */
sealed trait RestartStrategyProp extends FlinkProps derives JsonCodec

object RestartStrategyProp {

  @jsonField("none")
  case object NonRestart extends RestartStrategyProp:
    def mapping = Map(
      "restart-strategy" -> "none"
    ).resolve

  @jsonField("fixed-delay")
  case class FixedDelayRestart(attempts: Int = 1, delaySec: Int = 1) extends RestartStrategyProp:
    def mapping = Map(
      "restart-strategy"                      -> "fixed-delay",
      "restart-strategy.fixed-delay.attempts" -> attempts.ensureIntMin(1),
      "restart-strategy.fixed-delay.delay"    -> delaySec.ensureIntMin(1).contra(e => s"$e s")
    ).resolve

  @jsonField("failure-rate")
  case class FailureRateRestart(delaySec: Int = 1, failureRateIntervalSec: Int = 60, maxFailuresPerInterval: Int = 1) extends RestartStrategyProp:
    def mapping = Map(
      "restart-strategy"                                        -> "failure-rate",
      "restart-strategy.failure-rate.delay"                     -> failureRateIntervalSec.ensureIntMin(1).contra(e => s"$e s"),
      "restart-strategy.failure-rate.failure-rate-interval"     -> failureRateIntervalSec.ensureIntMin(1).contra(e => s"$e s"),
      "restart-strategy.failure-rate.max-failures-per-interval" -> maxFailuresPerInterval.ensureIntMin(1)
    ).resolve
}

/**
 * Flink state backend configuration.
 */
case class StateBackendProp(
    backendType: StateBackendType,
    checkpointStorage: CheckpointStorageType,
    checkpointDir: Option[String] = None,
    savepointDir: Option[String] = None,
    incremental: Boolean = false,
    localRecovery: Boolean = false,
    checkpointNumRetained: Int = 1)
    extends FlinkProps
    derives JsonCodec {

  def mapping: Map[String, String] = Map(
    "state.backend"                  -> backendType.rawValue,
    "state.checkpoint-storage"       -> checkpointStorage.rawValue,
    "state.checkpoints.dir"          -> checkpointDir,
    "state.savepoints.dir"           -> savepointDir,
    "state.backend.incremental"      -> incremental,
    "state.backend.local-recovery"   -> localRecovery,
    "state.checkpoints.num-retained" -> checkpointNumRetained.ensureIntMin(1),
  ).resolve
}

enum StateBackendType(val rawValue: String):
  case Hashmap extends StateBackendType("hashmap")
  case Rocksdb extends StateBackendType("rocksdb")

object StateBackendTypes:
  given JsonCodec[StateBackendType] = codecs.simpleEnumJsonCodec(StateBackendType.values)

enum CheckpointStorageType(val rawValue: String):
  case Jobmanager extends CheckpointStorageType("jobmanager")
  case Filesystem extends CheckpointStorageType("filesystem")

object CheckpointStorageTypes:
  given JsonCodec[CheckpointStorageType] = codecs.simpleEnumJsonCodec(CheckpointStorageType.values)

/**
 * Flink Jobmanager HA configuration.
 */
case class JmHaProp(
    haImplClass: String = "org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory",
    storageDir: String,
    clusterId: Option[String] = None)
    extends FlinkProps
    derives JsonCodec {

  def mapping: Map[String, String] = Map(
    "high-availability"            -> haImplClass,
    "high-availability.storageDir" -> storageDir,
    "high-availability.cluster-id" -> clusterId
  ).resolve
}

/**
 * s3 storage access configuration.
 */
case class S3AccessProp(
    endpoint: String,
    accessKey: String,
    secretKey: String,
    pathStyleAccess: Option[Boolean] = None,
    sslEnabled: Option[Boolean] = None)
    extends FlinkProps
    derives JsonCodec {

  def mapping: Map[String, String] = mappingS3

  /**
   * Mapping to standard flink-s3 configuration.
   */
  def mappingS3: Map[String, String] = Map(
    "s3.endpoint"          -> endpoint,
    "s3.access-key"        -> accessKey,
    "s3.secret-key"        -> secretKey,
    "s3.path.style.access" -> pathStyleAccess,
    "s3.ssl.enabled"       -> sslEnabled,
  ).resolve

  /**
   * Mapping to flink-s3-presto configuration.
   */
  def mappingS3p: Map[String, String] = Map(
    "hive.s3.endpoint"          -> endpoint,
    "hive.s3.aws-access-key"    -> accessKey,
    "hive.s3.aws-secret-key"    -> secretKey,
    "hive.s3.path-style-access" -> pathStyleAccess,
    "hive.s3.ssl.enabled"       -> sslEnabled,
  ).resolve

  /**
   * Mapping to flink-s3-hadoop configuration.
   */
  def mappingS3a: Map[String, String] = Map(
    "fs.s3a.endpoint"               -> endpoint,
    "fs.s3a.access.key"             -> accessKey,
    "fs.s3a.secret.key"             -> secretKey,
    "fs.s3a.path.style.access"      -> pathStyleAccess,
    "fs.s3a.connection.ssl.enabled" -> sslEnabled
  ).resolve
}

/**
 * Savepoint restore config.
 *
 * @see [[org.apache.flink.runtime.jobgraph.SavepointRestoreSettings]]
 */
case class SavepointRestoreProp(
    savepointPath: String,
    allowNonRestoredState: Boolean = false,
    restoreMode: SavepointRestoreMode = SavepointRestoreMode.Claim)
    extends FlinkProps
    derives JsonCodec {

  def mapping: Map[String, String] = Map(
    "execution.savepoint-restore-mode"           -> restoreMode.rawValue,
    "execution.savepoint.path"                   -> savepointPath,
    "execution.savepoint.ignore-unclaimed-state" -> allowNonRestoredState
  ).resolve
}

/**
 * @see [[org.apache.flink.runtime.jobgraph.RestoreMode]]
 */
enum SavepointRestoreMode(val rawValue: String):
  case Claim   extends SavepointRestoreMode("CLAIM")
  case NoClaim extends SavepointRestoreMode("NO_CLAIM")
  case Legacy  extends SavepointRestoreMode("LEGACY")

object SavepointRestoreModes:
  given JsonCodec[SavepointRestoreMode] = codecs.simpleEnumJsonCodec(SavepointRestoreMode.values)
