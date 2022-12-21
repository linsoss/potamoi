package potamoi.flink.operator

import potamoi.flink.*
import potamoi.flink.FlinkConfigExtension.given_Conversion_Configuration_ConfigurationPF
import potamoi.flink.FlinkErr.{ClusterNotFound, ConnectShardErr, K8sFail}
import potamoi.flink.FlinkRestErr.JobNotFound
import potamoi.flink.FlinkRestRequest.{StopJobSptReq, TriggerSptReq}
import potamoi.flink.model.*
import potamoi.flink.observer.FlinkObserver
import potamoi.fs.S3Conf
import potamoi.kubernetes.K8sErr.RequestK8sApiErr
import potamoi.kubernetes.K8sOperator
import potamoi.kubernetes.given

import zio.ZIO.{logInfo, succeed}
import zio.ZIOAspect.annotated
import zio.{IO, Task, UIO, ZIO, ZIOAspect}

import com.coralogix.zio.k8s.client.NotFound
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.DeleteOptions
import org.apache.flink.client.deployment.{ClusterClientFactory, DefaultClusterClientServiceLoader}
import org.apache.flink.configuration.Configuration

/**
 * Unified flink cluster operator.
 */
trait FlinkClusterUnifyOperator {

  /**
   * Terminate the flink cluster and reclaim all associated k8s resources.
   */
  def killCluster(fcid: Fcid): IO[FlinkErr, Unit]

  /**
   * Cancel job in flink session cluster.
   */
  def cancelJob(fjid: Fjid): IO[FlinkErr, Unit]

  /**
   * Stop job in flink session cluster with savepoint.
   */
  def stopJob(fjid: Fjid, savepoint: FlinkJobSavepointDef): IO[FlinkErr, (Fjid, TriggerId)]

  /**
   * Triggers a savepoint of flink job.
   */
  def triggerJobSavepoint(fjid: Fjid, savepoint: FlinkJobSavepointDef): IO[FlinkErr, (Fjid, TriggerId)]

}

/**
 * Default implementation.
 */
class FlinkClusterUnifyOperatorLive(flinkConf: FlinkConf, k8sOperator: K8sOperator, observer: FlinkObserver) extends FlinkClusterUnifyOperator {

  private given FlinkRestEndpointType    = flinkConf.restEndpointTypeInternal
  protected val flinkClusterClientLoader = new DefaultClusterClientServiceLoader()

  // Local workplace directory for each Flink cluster.
  protected def clusterLocalWorkspace(clusterId: String, namespace: String): UIO[String] =
    succeed(s"${flinkConf.localTmpDir}/${namespace}@${clusterId}")

  // Local Generated flink kubernetes pod-template file output path.
  protected def podTemplateFileOutputPath(clusterDef: FlinkClusterDef[_]): UIO[String] =
    clusterLocalWorkspace(clusterDef.clusterId, clusterDef.namespace).map(wp => s"$wp/flink-podtemplate.yaml")

  // Local Generated flink kubernetes config file output path.
  protected def logConfFileOutputPath(clusterDef: FlinkClusterDef[_]): UIO[String] =
    clusterLocalWorkspace(clusterDef.clusterId, clusterDef.namespace).map(wp => s"$wp/log-conf")

  // Get Flink ClusterClientFactory by execution mode.
  protected def getFlinkClusterClientFactory(execMode: FlinkExecMode): Task[ClusterClientFactory[String]] =
    ZIO.attempt {
      val conf = Configuration().append("execution.target", execMode.value)
      flinkClusterClientLoader.getClusterClientFactory(conf)
    }

  /**
   * Terminate the flink cluster and reclaim all associated k8s resources.
   */
  override def killCluster(fcid: Fcid): IO[DataStorageErr | ConnectShardErr | ClusterNotFound | K8sFail | FlinkErr, Unit] = {
    // untrack cluster
    observer.manager.untrack(fcid) *>
    // delete kubernetes resources
    k8sOperator.client.flatMap { client =>
      client.deployments
        .delete(name = fcid.clusterId, namespace = fcid.namespace, deleteOptions = DeleteOptions())
        .mapError {
          case NotFound => FlinkErr.ClusterNotFound(fcid)
          case failure  => FlinkErr.K8sFail(RequestK8sApiErr(failure))
        }
        .unit
    } *>
    logInfo(s"Delete flink cluster successfully.")
  } @@ annotated(fcid.toAnno: _*)

  /**
   * Cancel job in flink session cluster.
   */
  override def cancelJob(fjid: Fjid): IO[ClusterNotFound | JobNotFound | FlinkRestErr | FlinkErr, Unit] = {
    for {
      restUrl <- observer.restEndpoint.getEnsure(fjid.fcid).someOrFail(ClusterNotFound(fjid.fcid)).map(_.chooseUrl)
      _       <- flinkRest(restUrl).cancelJob(fjid.jobId)
    } yield ()
  } @@ annotated(fjid.toAnno: _*)

  /**
   * Stop job in flink session cluster with savepoint.
   */
  override def stopJob(
      fjid: Fjid,
      savepoint: FlinkJobSavepointDef): IO[ClusterNotFound | JobNotFound | FlinkRestErr | FlinkErr, (Fjid, TriggerId)] = {
    for {
      restUrl   <- observer.restEndpoint.getEnsure(fjid.fcid).someOrFail(ClusterNotFound(fjid.fcid)).map(_.chooseUrl)
      triggerId <- flinkRest(restUrl).stopJobWithSavepoint(fjid.jobId, StopJobSptReq(savepoint))
    } yield fjid -> triggerId
  } @@ annotated(fjid.toAnno: _*)

  /**
   * Triggers a savepoint of flink job.
   */
  override def triggerJobSavepoint(
      fjid: Fjid,
      savepoint: FlinkJobSavepointDef): IO[ClusterNotFound | JobNotFound | FlinkRestErr | FlinkErr, (Fjid, TriggerId)] = {
    for {
      restUrl   <- observer.restEndpoint.getEnsure(fjid.fcid).someOrFail(ClusterNotFound(fjid.fcid)).map(_.chooseUrl)
      triggerId <- flinkRest(restUrl).triggerSavepoint(fjid.jobId, TriggerSptReq(savepoint))
    } yield fjid -> triggerId
  } @@ annotated(fjid.toAnno: _*)

}
