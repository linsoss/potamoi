package potamoi.flink.operator

import potamoi.PotaErr
import potamoi.flink.*
import potamoi.flink.FlinkConfigExtension.{InjectedDeploySourceConf, InjectedExecModeKey, given}
import potamoi.flink.FlinkErr.{ClusterAlreadyExist, ClusterNotFound, SubmitFlinkClusterFail}
import potamoi.flink.FlinkRestErr.JobNotFound
import potamoi.flink.FlinkRestRequest.{RunJobReq, StopJobSptReq, TriggerSptReq}
import potamoi.flink.ResolveFlinkJobDefErr.{DownloadRemoteJobJarErr, NotSupportJobJarPath}
import potamoi.flink.model.*
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.operator.resolver.{ClusterDefResolver, LogConfigResolver, PodTemplateResolver}
import potamoi.fs.{lfs, S3Conf, S3Operator}
import potamoi.fs.PathTool.{getFileName, isS3Path}
import potamoi.kubernetes.K8sOperator
import potamoi.syntax.toPrettyStr
import potamoi.zios.usingAttempt
import zio.{Cause, IO, ZIO}
import zio.ZIO.{attempt, attemptBlockingInterrupt, logErrorCause, logInfo, scoped, succeed}
import zio.ZIOAspect.annotated

/**
 * Flink session mode cluster operator.
 */
trait FlinkSessClusterOperator extends FlinkClusterUnifyOperator {

  /**
   * Deploy Flink session cluster.
   */
  def deployCluster(definition: FlinkSessClusterDef): IO[FlinkErr, Unit]

  /**
   * Submit job to Flink session cluster.
   */
  def submitJob(definition: FlinkSessJobDef): IO[FlinkErr, JobId]
}

/**
 * Default implementation.
 */
class FlinkSessClusterOperatorLive(
    flinkConf: FlinkConf,
    s3Conf: S3Conf,
    k8sOperator: K8sOperator,
    s3Operator: S3Operator,
    observer: FlinkObserver)
    extends FlinkClusterUnifyOperatorLive(flinkConf, k8sOperator, observer) with FlinkSessClusterOperator {

  private given FlinkRestEndpointType = flinkConf.restEndpointTypeInternal

  /**
   * Deploy Flink session cluster.
   */
  override def deployCluster(
      clusterDef: FlinkSessClusterDef): IO[ClusterAlreadyExist | ResolveFlinkClusterDefErr | SubmitFlinkClusterFail | FlinkErr, Unit] = {
    for {
      _ <- existRemoteCluster(clusterDef.fcid).flatMap(ZIO.fail(ClusterAlreadyExist(clusterDef.fcid)).when(_))
      _ <- internalDeployCluster(clusterDef)
    } yield ()
  }.tapErrorCause { cause =>
    logErrorCause("Fail to deploy flink session cluster", cause).when(flinkConf.logFailedDeployReason)
  } @@ annotated(clusterDef.fcid.toAnno: _*)

  // noinspection DuplicatedCode
  private def internalDeployCluster(clusterDef: FlinkSessClusterDef): IO[ResolveFlinkClusterDefErr | SubmitFlinkClusterFail | FlinkErr, Unit] =
    for {
      clusterDef          <- ClusterDefResolver.session.revise(clusterDef)
      // resolve flink pod template and log config
      podTemplateFilePath <- podTemplateFileOutputPath(clusterDef)
      logConfFilePath     <- logConfFileOutputPath(clusterDef)
      _                   <- PodTemplateResolver.resolvePodTemplateAndDump(clusterDef, flinkConf, s3Conf, podTemplateFilePath)
      _                   <- LogConfigResolver.ensureFlinkLogsConfigFiles(logConfFilePath, overwrite = true)
      // convert to effective flink configuration
      rawConfig           <- ClusterDefResolver.session.toFlinkRawConfig(clusterDef, flinkConf, s3Conf).map { conf =>
                               conf
                                 .append("kubernetes.pod-template-file.jobmanager", podTemplateFilePath)
                                 .append("kubernetes.pod-template-file.taskmanager", podTemplateFilePath)
                                 .append("$internal.deployment.config-dir", logConfFilePath)
                                 .append(InjectedExecModeKey, FlinkTargetType.K8sSession)
                                 .append(InjectedDeploySourceConf._1, InjectedDeploySourceConf._2)
                             }
      _                   <- logInfo(s"Start to deploy flink session cluster:\n${rawConfig.toMap(true).toPrettyStr}")
      // deploy cluster
      _                   <- scoped {
                               for {
                                 clusterClientFactory <- getFlinkClusterClientFactory(FlinkTargetType.K8sSession)
                                 clusterSpecification <- attempt(clusterClientFactory.getClusterSpecification(rawConfig))
                                 k8sClusterDescriptor <- usingAttempt(clusterClientFactory.createClusterDescriptor(rawConfig))
                                 _                    <- attemptBlockingInterrupt(k8sClusterDescriptor.deploySessionCluster(clusterSpecification))
                               } yield ()
                             }.mapError(SubmitFlinkClusterFail(clusterDef.fcid, FlinkTargetType.K8sSession, _))
      // tracking cluster
      _                   <- observer.manager
                               .track(clusterDef.fcid)
                               .retryN(3)
                               .tapErrorCause(cause => logErrorCause(s"Failed to submit flink cluster trace request, need to trace manually later.", cause))
                               .ignore
      _                   <- logInfo(s"Deploy flink session cluster successfully.")
    } yield ()

  /**
   * Submit job to Flink session cluster.
   */
  override def submitJob(jobDef: FlinkSessJobDef): IO[ClusterNotFound | ResolveFlinkJobDefErr | FlinkRestErr | FlinkErr, JobId] = {
    for {
      // get rest api url of session cluster
      restUrl <- observer.restEndpoint.getEnsure(jobDef.fcid).someOrFail(ClusterNotFound(jobDef.fcid)).map(_.chooseUrl)
      _       <- logInfo(s"Connect flink rest service: $restUrl")
      _       <- ZIO.fail(NotSupportJobJarPath(jobDef.jobJar)).unless(isS3Path(jobDef.jobJar))

      // download job jar
      _          <- logInfo(s"Downloading flink job jar from s3 storage: ${jobDef.jobJar}")
      jobJarPath <-
        s3Operator
          .download(jobDef.jobJar, s"${flinkConf.localTmpDeployDir}/${jobDef.namespace}@${jobDef.clusterId}/${getFileName(jobDef.jobJar)}")
          .mapBoth(DownloadRemoteJobJarErr(jobDef.jobJar, _), _.getPath)

      // submit job
      _          <- logInfo(s"Start to submit job to flink cluster: \n${jobDef.toPrettyStr}".stripMargin)
      jobId      <- for {
                      _     <- logInfo(s"Uploading flink job jar to flink cluster, path: $jobJarPath, flink-rest: $restUrl")
                      rest   = flinkRest(restUrl)
                      jarId <- rest.uploadJar(jobJarPath)
                      jobId <- rest.runJar(jarId, RunJobReq(jobDef))
                      _     <- rest.deleteJar(jarId).ignore
                    } yield jobId

      _ <- lfs.rm(jobJarPath).ignore
      _ <- logInfo(s"Submit job to flink session cluster successfully, jobId: $jobId")
    } yield jobId
  }.tapErrorCause { cause =>
    logErrorCause("Fail to submit flink job to session cluster", cause).when(flinkConf.logFailedDeployReason)
  } @@ annotated(Fcid(jobDef.clusterId, jobDef.namespace).toAnno: _*)

}
