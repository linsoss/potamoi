package potamoi.flink

import potamoi.common.Err
import potamoi.flink.model.{Fcid, Fjid, FlinkExecMode, FlinkSessJobDef, JobState}
import potamoi.kubernetes.K8sErr

import scala.concurrent.duration.Duration

/**
 * Flink error.
 */
sealed abstract class FlinkErr(msg: String, cause: Throwable = null) extends Err(msg, cause)

object FlinkErr:
  case class K8sFail(err: K8sErr)                              extends FlinkErr(err.getMessage, err.getCause)
  case class ClusterNotFound(fcid: Fcid)                       extends FlinkErr(s"Flink cluster not found: ${fcid.show}")
  case class WatchTimeout(timeout: Duration)                   extends FlinkErr(s"Watch timeout with ${timeout.toString}")
  case class ConnectShardErr(entity: String, cause: Throwable) extends FlinkErr(s"Connect shard entity fail: $entity", cause)

  case class ClusterAlreadyExist(fcid: Fcid) extends FlinkErr(s"Flink cluster[${fcid.show}] already exists on kubernetes.")
  case class EmptyJobOnCluster(fcid: Fcid)   extends FlinkErr(s"There are no any jobs on the flink cluster: ${fcid.show}")
  case class SubmitFlinkClusterFail(fcid: Fcid, execMode: FlinkExecMode, cause: Throwable)
      extends FlinkErr(s"Fail to submit flink cluster to kubernetes: ${fcid.show}. execMode=${execMode.value}", cause)
  case class JobIsActive(fjid: Fjid, status: JobState)
      extends FlinkErr(s"Reject to submit application job, due to job[${fjid.jobId}] is [$status] on cluster[${fjid.fcid.show}]")

/**
 * Resolve flink cluster definition error.
 */
sealed abstract class ResolveClusterDefErr(msg: String, cause: Throwable) extends FlinkErr(msg, cause)

object ResolveClusterDefErr:
  case class ReviseClusterDefErr(cause: Throwable)                  extends ResolveClusterDefErr(s"Fail to revise flink cluster definition", cause)
  case class ConvertToRawConfigErr(cause: Throwable)                extends ResolveClusterDefErr(s"Fail to convert to flink raw configuration", cause)
  case class ResolveLogConfigErr(message: String, cause: Throwable) extends ResolveClusterDefErr(message, cause)
  case class ResolvePodTemplateErr(message: String, cause: Throwable) extends ResolveClusterDefErr(message, cause)

/**
 * Resolve flink job definition error.
 */
sealed abstract class ResolveJobDefErr(msg: String, cause: Throwable = null) extends FlinkErr(msg, cause)

object ResolveJobDefErr:
  case class NotSupportJobJarPath(path: String) extends ResolveJobDefErr(s"Unsupported flink jar path: $path")
  case class DownloadJobJarFail(remotePath: String, cause: Throwable)
      extends ResolveJobDefErr(s"Fail to download flink jar from remote: $remotePath", cause)

/**
 * Flink snapshot data storage operation err.
 */
sealed abstract class DataStorageErr(msg: String, cause: Throwable) extends FlinkErr(msg, cause)

object DataStorageErr:
  case class ReadDataErr(cause: Throwable)   extends DataStorageErr("Fail to read data from storage", cause)
  case class UpdateDataErr(cause: Throwable) extends DataStorageErr("Fail to update data to storage", cause)

/**
 * Flink rest api error
 */
sealed abstract class FlinkRestErr(msg: String, cause: Throwable = null) extends FlinkErr(msg, cause)

object FlinkRestErr:
  case class JarNotFound(jarId: String)         extends FlinkRestErr(s"Flink jar not found: jarId=$jarId")
  case class JobNotFound(jobId: String)         extends FlinkRestErr(s"Flink job not found: jarId=$jobId")
  case class TriggerNotFound(triggerId: String) extends FlinkRestErr(s"Flink trigger not found: triggerId=$triggerId")
  case class TaskmanagerNotFound(tmId: String)  extends FlinkRestErr(s"Flink task manager not found: tmId=$tmId")
  case class RequestApiErr(method: String, uri: String, cause: Throwable)
      extends FlinkRestErr(s"Fail to request flink rest api: method=$method, uri=$uri", cause)

/**
 * Flink ref k8s entity conversion error.
 */
sealed abstract class K8sEntityConvertErr(msg: String) extends FlinkErr(msg)

object K8sEntityConvertErr:
  case object IllegalK8sServiceEntity    extends K8sEntityConvertErr("Fail to convert kubernetes service entity")
  case object IllegalK8sDeploymentEntity extends K8sEntityConvertErr("Fail to convert kubernetes deployment entity")
  case object IllegalK8sPodEntity        extends K8sEntityConvertErr("Fail to convert kubernetes pod entity")
