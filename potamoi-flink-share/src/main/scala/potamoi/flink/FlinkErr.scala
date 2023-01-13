package potamoi.flink

import potamoi.PotaErr
import potamoi.flink.model.*
import potamoi.kubernetes.K8sErr

import scala.concurrent.duration.Duration

/**
 * Flink error.
 */
sealed trait FlinkErr extends PotaErr

object FlinkErr:

  case class K8sFailure(err: K8sErr)                                    extends FlinkErr
  case class WatchTimeout(timeout: Duration)                            extends FlinkErr
  case class FailToConnectShardEntity(entity: String, cause: Throwable) extends FlinkErr

  case class ClusterNotFound(fcid: Fcid)                                                     extends FlinkErr
  case class ClusterIsNotYetTracked(fcid: Fcid)                                              extends FlinkErr
  case class ClusterAlreadyExist(fcid: Fcid)                                                 extends FlinkErr
  case class EmptyJobOnCluster(fcid: Fcid)                                                   extends FlinkErr
  case class SubmitFlinkClusterFail(fcid: Fcid, execType: FlinkTargetType, cause: Throwable) extends FlinkErr
  case class JobAlreadyExist(fjid: Fjid, status: JobState)                                   extends FlinkErr

/**
 * Resolve flink cluster definition error.
 */
sealed trait ResolveClusterDefErr extends FlinkErr

object ResolveClusterDefErr:
  case class ReviseClusterDefErr(cause: Throwable)                    extends ResolveClusterDefErr
  case class ConvertToFlinkRawConfigErr(cause: Throwable)             extends ResolveClusterDefErr
  case class ResolveLogConfigErr(message: String, cause: Throwable)   extends ResolveClusterDefErr
  case class ResolvePodTemplateErr(message: String, cause: Throwable) extends ResolveClusterDefErr

/**
 * Resolve flink job definition error.
 */
sealed trait ResolveJobDefErr extends FlinkErr

object ResolveJobDefErr:
  case class NotSupportJobJarPath(path: String)                            extends FlinkErr
  case class DownloadRemoteJobJarErr(remotePath: String, cause: Throwable) extends FlinkErr

/**
 * Flink snapshot data storage operation err.
 */
sealed trait DataStoreErr extends FlinkErr

object DataStoreErr:
  case class ReadDataErr(cause: Throwable)   extends DataStoreErr
  case class UpdateDataErr(cause: Throwable) extends DataStoreErr

/**
 * Flink rest api error
 */
sealed trait FlinkRestErr extends FlinkErr

object FlinkRestErr:
  case class RequestApiErr(method: String, uri: String, cause: Throwable) extends FlinkRestErr

  sealed trait NotFound                         extends FlinkRestErr
  case class JarNotFound(jarId: String)         extends NotFound
  case class JobNotFound(jobId: String)         extends NotFound
  case class TriggerNotFound(triggerId: String) extends NotFound
  case class TaskmanagerNotFound(tmId: String)  extends NotFound

/**
 * Flink ref k8s entity conversion error.
 */
sealed trait FlinkK8sEntityConvertErr extends FlinkErr

object FlinkK8sEntityConvertErr:
  case object IllegalK8sServiceEntity    extends FlinkK8sEntityConvertErr
  case object IllegalK8sDeploymentEntity extends FlinkK8sEntityConvertErr
  case object IllegalK8sPodEntity        extends FlinkK8sEntityConvertErr

/**
 * Flink interactive operation error.
 */
