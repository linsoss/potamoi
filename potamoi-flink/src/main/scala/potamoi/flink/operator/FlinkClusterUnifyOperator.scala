package potamoi.flink.operator

import com.coralogix.zio.k8s.client.{DeserializationFailure, NotFound}
import com.coralogix.zio.k8s.client.model.K8sNamespace
import com.coralogix.zio.k8s.client.model.PropagationPolicy.{Background, Foreground}
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.DeleteOptions
import org.apache.flink.client.deployment.{ClusterClientFactory, DefaultClusterClientServiceLoader}
import org.apache.flink.configuration.Configuration
import potamoi.flink.*
import potamoi.flink.model.{Fcid, Fjid, FlinkTargetType}
import potamoi.flink.model.deploy.*
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.FlinkConfigurationTool.safeSet
import potamoi.flink.FlinkErr.{AkkaErr, ClusterNotFound, K8sFailure}
import potamoi.flink.FlinkRestErr.{JobNotFound, RequestApiErr}
import potamoi.flink.FlinkRestRequest.{StopJobSptReq, TriggerSptReq}
import potamoi.flink.observer.tracker.TrackerManager.TrackClusterErr
import potamoi.flink.operator.FlinkClusterUnifyOperator.*
import potamoi.flink.operator.resolver.{ClusterSpecResolver, LogConfigResolver, PodTemplateResolver}
import potamoi.kubernetes.{given_Conversion_String_K8sNamespace, K8sOperator}
import potamoi.kubernetes.K8sErr.RequestK8sApiErr
import potamoi.syntax.valueToSome
import potamoi.zios.{asLayer, repeatWhileWithSpaced, someOrFailUnion, union}
import potamoi.BaseConf
import potamoi.fs.{FileServerConf, FsBackendConf, RemoteFsOperator}
import zio.{durationInt, Clock, IO, Task, UIO, ZIO, ZLayer}
import zio.ZIO.logInfo
import zio.ZIOAspect.annotated
import zio.prelude.data.Optional.Present

/**
 * Unified flink cluster operator.
 */
trait FlinkClusterUnifyOperator {

  /**
   * Terminate the flink cluster and reclaim all associated k8s resources.
   */
  def killCluster(fcid: Fcid): IO[KillClusterErr, Unit]

  /**
   * Cancel job in flink session cluster.
   */
  def cancelJob(fjid: Fjid): IO[JobOperationErr, Unit]

  /**
   * Stop job in flink session cluster with savepoint.
   */
  def stopJob(fjid: Fjid, savepoint: JobSavepointSpec): IO[JobOperationErr, (Fjid, TriggerId)]

  /**
   * Triggers a savepoint of flink job.
   */
  def triggerJobSavepoint(fjid: Fjid, savepoint: JobSavepointSpec): IO[JobOperationErr, (Fjid, TriggerId)]
}

object FlinkClusterUnifyOperator {
  type KillClusterErr  = (ClusterNotFound | FlinkDataStoreErr | K8sFailure | AkkaErr) with FlinkErr
  type JobOperationErr = (ClusterNotFound | JobNotFound | FlinkDataStoreErr | K8sFailure | RequestApiErr) with FlinkErr
}

/**
 * Default implementation.
 */
class FlinkClusterUnifyOperatorImpl(
    flinkConf: FlinkConf,
    k8sOperator: K8sOperator,
    observer: FlinkObserver)
    extends FlinkClusterUnifyOperator {

  protected given FlinkRestEndpointType = flinkConf.restEndpointTypeInternal

  protected val flinkClusterClientLoader = new DefaultClusterClientServiceLoader()

  // Local workplace directory for each Flink cluster.
  protected def localWorkspace(fcid: Fcid): UIO[String] =
    ZIO.succeed(s"${flinkConf.localTmpDeployDir}/${fcid.namespace}@${fcid.clusterId}")

  // Local Generated flink kubernetes pod-template file output path.
  protected def podTemplateFileOutputPath(fcid: Fcid): UIO[String] =
    localWorkspace(fcid.clusterId, fcid.namespace).map(wp => s"$wp/flink-podtemplate.yaml")

  // Local Generated flink kubernetes config file output path.
  protected def logConfFileOutputPath(fcid: Fcid): UIO[String] =
    localWorkspace(fcid.clusterId, fcid.namespace).map(wp => s"$wp/log-conf")

  // Get Flink ClusterClientFactory by execution mode.
  protected def getFlinkClusterClientFactory(execType: FlinkTargetType): Task[ClusterClientFactory[String]] =
    ZIO.attempt {
      val conf = Configuration().safeSet("execution.target", execType.rawValue)
      flinkClusterClientLoader.getClusterClientFactory(conf)
    }

  /**
   * Determine if a flink cluster is alive or not.
   */
  protected def isRemoteClusterAlive(fcid: Fcid): IO[FlinkDataStoreErr | K8sFailure, Boolean] =
    observer.manager.isBeTracked(fcid).flatMap {
      case true  => observer.k8s.deployment.listName(fcid).map(_.nonEmpty)
      case false => observer.restEndpoint.retrieve(fcid).map(_.isDefined)
    }

  /**
   * Terminate the flink cluster and reclaim all associated k8s resources.
   */
  override def killCluster(fcid: Fcid): IO[KillClusterErr, Unit] =
    union[KillClusterErr, Unit] {
      for {
        // untrack cluster
        _ <- observer.manager.untrack(fcid).retryN(3)
        // delete kubernetes resources
        _ <- internalKillCluster(fcid, wait = false)
        _ <- logInfo(s"Delete flink cluster successfully.")
      } yield ()
    } @@ annotated(fcid.toAnno: _*)

  protected def internalKillCluster(fcid: Fcid, wait: Boolean): IO[FlinkDataStoreErr | ClusterNotFound | K8sFailure, Unit] = {
    if wait then
      k8sOperator.client.deployments
        .deleteAndWait(
          name = fcid.clusterId,
          namespace = fcid.namespace,
          deleteOptions = DeleteOptions(propagationPolicy = Present("Background"))
        )
        .mapError[FlinkErr.ClusterNotFound | FlinkErr.K8sFailure] {
          case NotFound => FlinkErr.ClusterNotFound(fcid)
          case failure  => FlinkErr.K8sFailure(RequestK8sApiErr(failure))
        }
        .unit
        .provide(ZLayer.succeed(Clock.ClockLive)) *>
      // wait for rest resources to be destroyed.
      k8sOperator.client.services
        .get(name = fcid.clusterId + "-rest", namespace = fcid.namespace)
        .as(true)
        .catchSome { case NotFound => ZIO.succeed(false) }
        .repeatWhileWithSpaced(e => e, 500.millis)
        .mapError(e => FlinkErr.K8sFailure(RequestK8sApiErr(e)))
        .unit
    else
      k8sOperator.client.deployments
        .delete(
          name = fcid.clusterId,
          namespace = fcid.namespace,
          deleteOptions = DeleteOptions(propagationPolicy = Present("Background"))
        )
        .mapError[FlinkErr.ClusterNotFound | FlinkErr.K8sFailure] {
          case NotFound => FlinkErr.ClusterNotFound(fcid)
          case failure  => FlinkErr.K8sFailure(RequestK8sApiErr(failure))
        }
        .unit
  }

  /**
   * Cancel job in flink session cluster.
   */
  override def cancelJob(fjid: Fjid): IO[JobOperationErr, Unit] =
    union[JobOperationErr, Unit] {
      for {
        restUrl <- observer.restEndpoint
                     .getEnsure(fjid.fcid)
                     .someOrFailUnion(ClusterNotFound(fjid.fcid))
                     .map(_.chooseUrl)
        _       <- flinkRest(restUrl).cancelJob(fjid.jobId)
      } yield ()
    } @@ annotated(fjid.toAnno: _*)

  /**
   * Stop job in flink session cluster with savepoint.
   */
  override def stopJob(fjid: Fjid, savepoint: JobSavepointSpec): IO[JobOperationErr, (Fjid, TriggerId)] =
    union[JobOperationErr, (Fjid, TriggerId)] {
      for {
        restUrl   <- observer.restEndpoint
                       .getEnsure(fjid.fcid)
                       .someOrFailUnion(ClusterNotFound(fjid.fcid))
                       .map(_.chooseUrl)
        triggerId <- flinkRest(restUrl).stopJobWithSavepoint(fjid.jobId, StopJobSptReq(savepoint))
      } yield fjid -> triggerId
    } @@ annotated(fjid.toAnno: _*)

  /**
   * Triggers a savepoint of flink job.
   */
  override def triggerJobSavepoint(fjid: Fjid, savepoint: JobSavepointSpec): IO[JobOperationErr, (Fjid, TriggerId)] =
    union[JobOperationErr, (Fjid, TriggerId)] {
      for {
        restUrl   <- observer.restEndpoint
                       .getEnsure(fjid.fcid)
                       .someOrFailUnion(ClusterNotFound(fjid.fcid))
                       .map(_.chooseUrl)
        triggerId <- flinkRest(restUrl).triggerSavepoint(fjid.jobId, TriggerSptReq(savepoint))
      } yield fjid -> triggerId
    } @@ annotated(fjid.toAnno: _*)

}
