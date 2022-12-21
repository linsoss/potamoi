package potamoi.flink.model

import potamoi.fs.{S3AccessStyle, S3Conf}
import potamoi.syntax.contra
import zio.json.{jsonField, DeriveJsonCodec, JsonCodec, JsonDecoder, JsonEncoder}
import potamoi.nums.*

/**
 * Type-safe flink major configuration entries.
 */
sealed trait FlinkRawConfig {

  /**
   * convert to flink raw configuration.
   */
  def mapping: Map[String, String]
}

object FlinkRawConfig {

  import StateBackendTypes.given
  import CheckpointStorageTypes.given
  import RestExportTypes.given
  import SavepointRestoreModes.given

  given JsonCodec[CpuConfig]              = DeriveJsonCodec.gen[CpuConfig]
  given JsonCodec[MemConfig]              = DeriveJsonCodec.gen[MemConfig]
  given JsonCodec[ParConfig]              = DeriveJsonCodec.gen[ParConfig]
  given JsonCodec[WebUIConfig]            = DeriveJsonCodec.gen[WebUIConfig]
  given JsonCodec[RestartStgConfig]       = DeriveJsonCodec.gen[RestartStgConfig]
  given JsonCodec[StateBackendConfig]     = DeriveJsonCodec.gen[StateBackendConfig]
  given JsonCodec[JmHaConfig]             = DeriveJsonCodec.gen[JmHaConfig]
  given JsonCodec[S3AccessConf]           = DeriveJsonCodec.gen[S3AccessConf]
  given JsonCodec[SavepointRestoreConfig] = DeriveJsonCodec.gen[SavepointRestoreConfig]

  /**
   * Eliminate empty configuration items and convert to String format.
   */
  extension (items: Map[String, _])
    def dry: Map[String, String] = items
      .filter { case (_, value) =>
        value match
          case None                     => false
          case value: Iterable[_]       => value.nonEmpty
          case Some(value: Iterable[_]) => value.nonEmpty
          case _                        => true
      }
      .map {
        case (key, Some(value)) => key -> value
        case (k, v)             => k   -> v
      }
      .map {
        case (k, value: Map[_, _])   => k -> value.map(kv => s"${kv._1}:${kv._2}").mkString(",")
        case (k, value: Iterable[_]) => k -> value.mkString(",")
        case (k, v)                  => k -> v.toString
      }
}

import FlinkRawConfig.dry

/**
 * Flink k8s cpu configuration.
 */
case class CpuConfig(jm: Double = 1.0, tm: Double = -1.0, jmFactor: Double = 1.0, tmFactor: Double = 1.0) extends FlinkRawConfig:
  def mapping = Map(
    "kubernetes.taskmanager.cpu"              -> jm.ensureDoubleOr(_ > 0, 1.0),
    "kubernetes.jobmanager.cpu.limit-factor"  -> jmFactor.ensureDoubleOr(_ > 0, 1.0),
    "kubernetes.taskmanager.cpu"              -> tm,
    "kubernetes.taskmanager.cpu.limit-factor" -> tmFactor.ensureDoubleOr(_ > 0, 1.0)
  ).dry

/**
 * Flink parallelism configuration.
 */
case class ParConfig(numOfSlot: Int = 1, parDefault: Int = 1) extends FlinkRawConfig:
  def mapping = Map(
    "taskmanager.numberOfTaskSlots" -> numOfSlot.ensureIntMin(1),
    "parallelism.default"           -> parDefault.ensureIntMin(1)
  ).dry

/**
 * Flink memory configuration.
 */
case class MemConfig(jmMB: Int = 1920, tmMB: Int = 1920) extends FlinkRawConfig:
  def mapping = Map(
    "jobmanager.memory.process.size"  -> jmMB.ensureIntOr(_ > 0, 1920).contra(_ + "m"),
    "taskmanager.memory.process.size" -> tmMB.ensureIntOr(_ > 0, 1920).contra(_ + "m")
  ).dry

/**
 * Flink web ui service configuration.
 */
case class WebUIConfig(enableSubmit: Boolean = true, enableCancel: Boolean = true) extends FlinkRawConfig:
  def mapping = Map(
    "web.submit.enable" -> enableSubmit,
    "web.cancel.enable" -> enableCancel
  ).dry

/**
 * Flink task restart strategy.
 */
sealed trait RestartStgConfig extends FlinkRawConfig

@jsonField("none")
case object NonRestartStg extends RestartStgConfig:
  def mapping = Map("restart-strategy" -> "none").dry

@jsonField("fixed-delay")
case class FixedDelayRestartStg(attempts: Int = 1, delaySec: Int = 1) extends RestartStgConfig:
  def mapping = Map(
    "restart-strategy"                      -> "fixed-delay",
    "restart-strategy.fixed-delay.attempts" -> attempts.ensureIntMin(1),
    "restart-strategy.fixed-delay.delay"    -> delaySec.ensureIntMin(1).contra(e => s"$e s")
  ).dry

@jsonField("failure-rate")
case class FailureRateRestartStg(delaySec: Int = 1, failureRateIntervalSec: Int = 60, maxFailuresPerInterval: Int = 1) extends RestartStgConfig {
  def mapping = Map(
    "restart-strategy"                                        -> "failure-rate",
    "restart-strategy.failure-rate.delay"                     -> failureRateIntervalSec.ensureIntMin(1).contra(e => s"$e s"),
    "restart-strategy.failure-rate.failure-rate-interval"     -> failureRateIntervalSec.ensureIntMin(1).contra(e => s"$e s"),
    "restart-strategy.failure-rate.max-failures-per-interval" -> maxFailuresPerInterval.ensureIntMin(1)
  ).dry
}

/**
 * Flink state backend configuration.
 */
case class StateBackendConfig(
    backendType: StateBackendType,
    checkpointStorage: CheckpointStorageType,
    checkpointDir: Option[String] = None,
    savepointDir: Option[String] = None,
    incremental: Boolean = false,
    localRecovery: Boolean = false,
    checkpointNumRetained: Int = 1)
    extends FlinkRawConfig:
  def mapping = Map(
    "state.backend"                  -> backendType.value,
    "state.checkpoint-storage"       -> checkpointStorage.value,
    "state.checkpoints.dir"          -> checkpointDir,
    "state.savepoints.dir"           -> savepointDir,
    "state.backend.incremental"      -> incremental,
    "state.backend.local-recovery"   -> localRecovery,
    "state.checkpoints.num-retained" -> checkpointNumRetained.ensureIntMin(1),
  ).dry

enum StateBackendType(val value: String):
  case Hashmap extends StateBackendType("hashmap")
  case Rocksdb extends StateBackendType("rocksdb")

object StateBackendTypes:
  given JsonCodec[StateBackendType] = JsonCodec(
    JsonEncoder[String].contramap(_.value),
    JsonDecoder[String].mapOrFail(s => StateBackendType.values.find(_.value == s).toRight(s"Unknown state backend type: $s"))
  )

enum CheckpointStorageType(val value: String):
  case Jobmanager extends CheckpointStorageType("jobmanager")
  case Filesystem extends CheckpointStorageType("filesystem")

object CheckpointStorageTypes:
  given JsonCodec[CheckpointStorageType] = JsonCodec(
    JsonEncoder[String].contramap(_.value),
    JsonDecoder[String].mapOrFail(s => CheckpointStorageType.values.find(_.value == s).toRight(s"Unknown checkpoint storage type: $s"))
  )

/**
 * Flink Jobmanager HA configuration.
 */
case class JmHaConfig(
    haImplClz: String = "org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory",
    storageDir: String,
    clusterId: Option[String] = None)
    extends FlinkRawConfig:
  def mapping = Map(
    "high-availability"            -> haImplClz,
    "high-availability.storageDir" -> storageDir,
    "high-availability.cluster-id" -> clusterId
  ).dry

/**
 * s3 storage access configuration.
 */
case class S3AccessConf(
    endpoint: String,
    accessKey: String,
    secretKey: String,
    pathStyleAccess: Option[Boolean] = None,
    sslEnabled: Option[Boolean] = None) {

  /**
   * Mapping to flink-s3-presto configuration.
   */
  def mappingS3p = Map(
    "hive.s3.endpoint"          -> endpoint,
    "hive.s3.aws-access-key"    -> accessKey,
    "hive.s3.aws-secret-key"    -> secretKey,
    "hive.s3.path-style-access" -> pathStyleAccess,
    "hive.s3.ssl.enabled"       -> sslEnabled
  ).dry

  /**
   * Mapping to flink-s3-hadoop configuration.
   */
  def mappingS3a = Map(
    "fs.s3a.endpoint"               -> endpoint,
    "fs.s3a.access.key"             -> accessKey,
    "fs.s3a.secret.key"             -> secretKey,
    "fs.s3a.path.style.access"      -> pathStyleAccess,
    "fs.s3a.connection.ssl.enabled" -> sslEnabled
  ).dry
}

object S3AccessConf:
  def apply(conf: S3Conf): S3AccessConf =
    S3AccessConf(conf.endpoint, conf.accessKey, conf.secretKey, Some(conf.accessStyle == S3AccessStyle.PathStyle), Some(conf.sslEnabled))

enum RestExportType(val value: String):
  case ClusterIP         extends RestExportType("ClusterIP")
  case NodePort          extends RestExportType("NodePort")
  case LoadBalancer      extends RestExportType("LoadBalancer")
  case HeadlessClusterIP extends RestExportType("Headless_ClusterIP")

object RestExportTypes:
  given JsonCodec[RestExportType] = JsonCodec(
    JsonEncoder[String].contramap(_.value),
    JsonDecoder[String].mapOrFail(s => RestExportType.values.find(_.value == s).toRight(s"Unknown rest export type: $s"))
  )

/**
 * Savepoint restore config.
 *
 * @see [[org.apache.flink.runtime.jobgraph.SavepointRestoreSettings]]
 */
case class SavepointRestoreConfig(
    savepointPath: String,
    allowNonRestoredState: Boolean = false,
    restoreMode: SavepointRestoreMode = SavepointRestoreMode.Claim)
    extends FlinkRawConfig:
  def mapping = Map(
    "execution.savepoint-restore-mode"           -> restoreMode.value,
    "execution.savepoint.path"                   -> savepointPath,
    "execution.savepoint.ignore-unclaimed-state" -> allowNonRestoredState
  ).dry

/**
 * @see [[org.apache.flink.runtime.jobgraph.RestoreMode]]
 */
enum SavepointRestoreMode(val value: String):
  case Claim   extends SavepointRestoreMode("CLAIM")
  case NoClaim extends SavepointRestoreMode("NO_CLAIM")
  case Legacy  extends SavepointRestoreMode("LEGACY")

object SavepointRestoreModes:
  given JsonCodec[SavepointRestoreMode] = JsonCodec(
    JsonEncoder[String].contramap(_.value),
    JsonDecoder[String].mapOrFail(s => SavepointRestoreMode.values.find(_.value == s).toRight(s"Unknown savepoint restore mode: $s"))
  )
