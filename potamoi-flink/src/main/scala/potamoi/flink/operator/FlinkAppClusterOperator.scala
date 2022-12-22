package potamoi.flink.operator

import com.coralogix.zio.k8s.client.NotFound
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.DeleteOptions
import org.apache.flink.client.deployment.application.ApplicationConfiguration
import potamoi.errs.{headMessage, recurse}
import potamoi.flink.*
import potamoi.flink.FlinkConfigExtension.{InjectedDeploySourceConf, InjectedExecModeKey, given}
import potamoi.flink.FlinkErr.{ClusterNotFound, EmptyJobOnCluster, JobIsActive, SubmitFlinkClusterFail}
import potamoi.flink.FlinkRestErr.JobNotFound
import potamoi.flink.model.*
import potamoi.flink.model.FlinkExecMode.*
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.operator.resolver.{ClusterDefResolver, LogConfigResolver, PodTemplateResolver}
import potamoi.flink.FlinkRestRequest.{JobStatusInfo, RunJobReq, StopJobSptReq, TriggerSptReq}
import potamoi.fs.{S3Conf, S3Operator}
import potamoi.fs.PathTool.{getFileName, isS3Path}
import potamoi.kubernetes.K8sErr.RequestK8sApiErr
import potamoi.kubernetes.K8sOperator
import potamoi.syntax.toPrettyStr
import potamoi.zios.usingAttempt
import zio.{durationInt, Console, IO, ZIO}
import zio.Console.printLine
import zio.Schedule.{recurWhile, spaced}
import zio.ZIO.{attempt, attemptBlockingInterrupt, logErrorCause, logInfo, scoped, succeed}
import zio.ZIOAspect.annotated
import potamoi.kubernetes.given_Conversion_String_K8sNamespace

/**
 * Flink application mode cluster operator.
 */
trait FlinkAppClusterOperator extends FlinkClusterUnifyOperator {

  /**
   * Deploy Flink application cluster.
   */
  def deployCluster(definition: FlinkAppClusterDef): IO[FlinkErr, Unit]

  /**
   * Cancel job in flink application cluster.
   */
  def cancelJob(fcid: Fcid): IO[FlinkErr, Unit]

  /**
   * Stop job in flink application cluster with savepoint.
   */
  def stopJob(fcid: Fcid, savepoint: FlinkJobSavepointDef): IO[FlinkErr, (Fjid, TriggerId)]

  /**
   * Triggers a savepoint of flink job.
   */
  def triggerJobSavepoint(fcid: Fcid, savepoint: FlinkJobSavepointDef): IO[FlinkErr, (Fjid, TriggerId)]
}

/**
 * Default implementation.
 */
case class FlinkAppClusterOperatorLive(
    flinkConf: FlinkConf,
    s3Conf: S3Conf,
    k8sOperator: K8sOperator,
    s3Operator: S3Operator,
    observer: FlinkObserver)
    extends FlinkClusterUnifyOperatorLive(flinkConf, k8sOperator, observer) with FlinkAppClusterOperator {

  private given FlinkRestEndpointType = flinkConf.restEndpointTypeInternal

  /**
   * Deploy Flink session cluster.
   */

  override def deployCluster(clusterDef: FlinkAppClusterDef): IO[JobIsActive | ResolveClusterDefErr | SubmitFlinkClusterFail | FlinkErr, Unit] = {
    for {
      _ <- ensureRemoteEnvReady(clusterDef.fcid)
      _ <- internalDeployCluster(clusterDef)
    } yield ()
  }.tapErrorCause(cause =>
    ZIO
      .logErrorCause(s"Fail to deploy flink application cluster due to: ${cause.headMessage}", cause.recurse)
      .when(flinkConf.logFailedDeployReason))
  @@ annotated (clusterDef.fcid.toAnno: _*)

  // Delete the flink cluster when no job exists in the flink cluster
  // or when the only one job existed is inactive.
  private def ensureRemoteEnvReady(fcid: Fcid): IO[FlinkErr, Unit] =
    existRemoteCluster(fcid).flatMap {
      case false => ZIO.unit
      case true =>
        clearCluster(fcid)
//        observer.restEndpoint.getEnsure(fcid).flatMap {
//          case None => clearCluster(fcid)
//          case Some(ept) =>
//            flinkRest(ept.chooseUrl).listJobsStatusInfo.map(_.headOption).catchAll(_ => ZIO.succeed(None)).flatMap {
//              case None => clearCluster(fcid)
//              case Some(JobStatusInfo(jobId, state)) =>
//                if !JobStates.isActive(state) then clearCluster(fcid) else ZIO.fail(JobIsActive(Fjid(fcid, jobId), state))
//            }
//        }
    }

  private inline def clearCluster(fcid: Fcid) = {
    logInfo(s"Deleting flink k8s resources...") *>
    internalKillCluster(fcid, wait = true) *>
//    ZIO.sleep(5.seconds) *>
    logInfo(s"Flink k8s resources deleted.")
  }

  // noinspection DuplicatedCode
  private def internalDeployCluster(clusterDef: FlinkAppClusterDef): IO[ResolveClusterDefErr | SubmitFlinkClusterFail | FlinkErr, Unit] =
    for {
      clusterDef <- ClusterDefResolver.application.revise(clusterDef)
      // resolve flink pod template and log config
      podTemplateFilePath <- podTemplateFileOutputPath(clusterDef)
      logConfFilePath     <- logConfFileOutputPath(clusterDef)
      _                   <- PodTemplateResolver.resolvePodTemplateAndDump(clusterDef, flinkConf, s3Conf, podTemplateFilePath)
      _                   <- LogConfigResolver.ensureFlinkLogsConfigFiles(logConfFilePath, overwrite = true)
      // convert to effective flink configuration
      rawConfig <- ClusterDefResolver.application.toFlinkRawConfig(clusterDef, flinkConf, s3Conf).map { conf =>
        conf
          .append("kubernetes.pod-template-file.jobmanager", podTemplateFilePath)
          .append("kubernetes.pod-template-file.taskmanager", podTemplateFilePath)
          .append("$internal.deployment.config-dir", logConfFilePath)
          .append("execution.shutdown-on-application-finish", false) // prevents the jobmanager from being destroyed
          .append("execution.shutdown-on-attached-exit", false)
          .append(InjectedExecModeKey, K8sApplication)
          .append(InjectedDeploySourceConf._1, InjectedDeploySourceConf._2)
      }
      _ <- logInfo(s"Start to deploy flink application cluster:\n${rawConfig.toMap(true).toPrettyStr}".stripMargin)
      // deploy app cluster
      _ <- scoped {
        for {
          clusterClientFactory <- getFlinkClusterClientFactory(K8sApplication)
          clusterSpecification <- attempt(clusterClientFactory.getClusterSpecification(rawConfig))
          appConfiguration     <- attempt(new ApplicationConfiguration(clusterDef.appArgs.toArray, clusterDef.appMain.orNull))
          k8sClusterDescriptor <- usingAttempt(clusterClientFactory.createClusterDescriptor(rawConfig))
          _                    <- attemptBlockingInterrupt(k8sClusterDescriptor.deployApplicationCluster(clusterSpecification, appConfiguration))
        } yield ()
      }.mapError(SubmitFlinkClusterFail(clusterDef.fcid, K8sApplication, _))
      // tracking cluster
      _ <- observer.manager
        .track(clusterDef.fcid)
        .retryN(3)
        .tapErrorCause(cause => logErrorCause(s"Failed to submit flink cluster trace request, need to trace manually later.", cause.recurse))
        .ignore
      _ <- logInfo(s"Deploy flink application cluster successfully.")
    } yield ()

  /**
   * Cancel job in flink application cluster.
   */
  override def cancelJob(fcid: Fcid): IO[ClusterNotFound | EmptyJobOnCluster | JobNotFound | FlinkRestErr | FlinkErr, Unit] = {
    for {
      restUrl <- observer.restEndpoint.getEnsure(fcid).someOrFail(ClusterNotFound(fcid)).map(_.chooseUrl)
      jobId   <- findFirstJobId(fcid, restUrl)
      _       <- flinkRest(restUrl).cancelJob(jobId)
    } yield ()
  } @@ annotated(fcid.toAnno: _*)

  /**
   * Stop job in flink application cluster with savepoint.
   */
  override def stopJob(
      fcid: Fcid,
      savepoint: FlinkJobSavepointDef): IO[ClusterNotFound | EmptyJobOnCluster | JobNotFound | FlinkRestErr | FlinkErr, (Fjid, TriggerId)] = {
    for {
      restUrl   <- observer.restEndpoint.getEnsure(fcid).someOrFail(ClusterNotFound(fcid)).map(_.chooseUrl)
      jobId     <- findFirstJobId(fcid, restUrl)
      triggerId <- flinkRest(restUrl).stopJobWithSavepoint(jobId, StopJobSptReq(savepoint))
    } yield Fjid(fcid, jobId) -> triggerId
  } @@ annotated(fcid.toAnno: _*)

  /**
   * Triggers a savepoint of flink job.
   */
  override def triggerJobSavepoint(
      fcid: Fcid,
      savepoint: FlinkJobSavepointDef): IO[ClusterNotFound | EmptyJobOnCluster | JobNotFound | FlinkRestErr | FlinkErr, (Fjid, TriggerId)] = {
    for {
      restUrl   <- observer.restEndpoint.getEnsure(fcid).someOrFail(ClusterNotFound(fcid)).map(_.chooseUrl)
      jobId     <- findFirstJobId(fcid, restUrl)
      triggerId <- flinkRest(restUrl).triggerSavepoint(jobId, TriggerSptReq(savepoint))
    } yield Fjid(fcid, jobId) -> triggerId
  } @@ annotated(fcid.toAnno: _*)

  private def findFirstJobId(fcid: Fcid, restUrl: String) =
    flinkRest(restUrl).listJobsStatusInfo.map(_.headOption.map(_.id)).someOrFail(EmptyJobOnCluster(fcid))
}
