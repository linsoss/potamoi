package potamoi.flink

import potamoi.{KryoSerializable, PotaErr}
import potamoi.akka.ActorOpErr
import potamoi.flink.FlinkInterpreterErr.{RetrieveResultNothing, SplitSqlScriptErr}
import potamoi.flink.model.*
import potamoi.flink.model.snapshot.JobState
import potamoi.kubernetes.K8sErr

import scala.concurrent.duration.Duration

/**
 * Flink error.
 */
sealed trait FlinkErr extends PotaErr with KryoSerializable

object FlinkErr:

  case class K8sFailure(err: K8sErr)         extends FlinkErr
  case class WatchTimeout(timeout: Duration) extends FlinkErr
  case class AkkaErr(reason: ActorOpErr)     extends FlinkErr

  case class ClusterNotFound(fcid: Fcid)                                                     extends FlinkErr
  case class ClusterIsNotYetTracked(fcid: Fcid)                                              extends FlinkErr
  case class ClusterAlreadyExist(fcid: Fcid)                                                 extends FlinkErr
  case class EmptyJobOnCluster(fcid: Fcid)                                                   extends FlinkErr
  case class SubmitFlinkClusterFail(fcid: Fcid, execType: FlinkTargetType, cause: Throwable) extends FlinkErr
  case class JobAlreadyExist(fjid: Fjid, status: JobState)                                   extends FlinkErr

/**
 * Resolve flink cluster definition error.
 */
sealed trait ResolveFlinkClusterSpecErr extends FlinkErr

object ResolveFlinkClusterSpecErr:
  case class ReviseClusterSpecErr(cause: Throwable)                   extends ResolveFlinkClusterSpecErr
  case class ConvertToFlinkRawConfigErr(cause: Throwable)             extends ResolveFlinkClusterSpecErr
  case class ResolveLogConfigErr(message: String, cause: Throwable)   extends ResolveFlinkClusterSpecErr
  case class ResolvePodTemplateErr(message: String, cause: Throwable) extends ResolveFlinkClusterSpecErr

/**
 * Resolve flink job definition error.
 */
sealed trait ResolveFlinkJobSpecErr extends FlinkErr

object ResolveFlinkJobSpecErr:
  case class NotSupportJobJarPath(path: String)                            extends ResolveFlinkJobSpecErr
  case class DownloadRemoteJobJarErr(remotePath: String, cause: Throwable) extends ResolveFlinkJobSpecErr

/**
 * Flink snapshot data storage operation err.
 */
sealed trait FlinkDataStoreErr extends FlinkErr

object FlinkDataStoreErr:
  case class ReadDataErr(cause: Throwable)   extends FlinkDataStoreErr
  case class UpdateDataErr(cause: Throwable) extends FlinkDataStoreErr

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
sealed trait FlinkInteractErr extends FlinkErr

object FlinkInteractErr:
  case class RemoteInterpreterNotYetLaunch(flinkVer: FlinkMajorVer)     extends FlinkInteractErr
  case class SessionNotYetStarted(sessionId: String)                    extends FlinkInteractErr
  case class SessionNotFound(sessionId: String)                         extends FlinkInteractErr
  case class SessionHandleNotFound(sessionId: String, handleId: String) extends FlinkInteractErr

  case class ResolveFlinkClusterEndpointErr(fcid: Fcid, reason: FlinkErr)   extends FlinkInteractErr
  case class FailToSplitSqlScript(reason: SplitSqlScriptErr, stack: String) extends FlinkInteractErr

/**
 * Flink sql interpreter error.
 */
sealed trait FlinkInterpreterErr extends PotaErr with KryoSerializable

object FlinkInterpreterErr:
  case class SplitSqlScriptErr(cause: Throwable) extends FlinkInterpreterErr

  sealed trait RetrieveResultNothing          extends FlinkInterpreterErr
  case class HandleNotFound(handleId: String) extends RetrieveResultNothing
  case class ResultNotFound(handleId: String) extends RetrieveResultNothing

  sealed trait ExecuteSqlErr                                       extends FlinkInterpreterErr
  case class CreateTableEnvironmentErr(cause: Throwable)           extends ExecuteSqlErr
  case class ParseSqlErr(sql: String, cause: Throwable)            extends ExecuteSqlErr
  case class BannedOperation(opClzName: String)                    extends ExecuteSqlErr
  case class ExecOperationErr(opClzName: String, cause: Throwable) extends ExecuteSqlErr
  case class BeCancelled(handleId: String)                         extends ExecuteSqlErr
