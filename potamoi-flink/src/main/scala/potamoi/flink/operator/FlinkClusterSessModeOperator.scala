package potamoi.flink.operator

import potamoi.flink.*
import potamoi.flink.model.deploy.{SessionClusterSpec, SessionJobSpec}
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.operator.resolver.{ClusterSpecResolver, LogConfigResolver, PodTemplateResolver}
import potamoi.kubernetes.K8sOperator
import potamoi.BaseConf
import potamoi.flink.FlinkConfigurationTool.dumpToMap
import potamoi.flink.model.FlinkTargetType
import potamoi.flink.FlinkErr.{ClusterAlreadyExist, ClusterNotFound, K8sFailure, SubmitFlinkClusterFail}
import potamoi.flink.ResolveFlinkClusterSpecErr.{ConvertToFlinkRawConfigErr, ResolveLogConfigErr, ResolvePodTemplateErr, ReviseClusterSpecErr}
import potamoi.flink.operator.FlinkSessionModeOperator.*
import potamoi.flink.FlinkRestErr.RequestApiErr
import potamoi.flink.FlinkRestRequest.RunJobReq
import potamoi.flink.ResolveFlinkJobSpecErr.{DownloadRemoteJobJarErr, NotSupportJobJarPath}
import potamoi.fs.*
import potamoi.fs.PathTool.isS3Path
import potamoi.syntax.toPrettyStr
import potamoi.zios.{asLayer, someOrFailUnion, union, usingAttempt}
import zio.{IO, ZIO}
import zio.ZIO.{attempt, attemptBlockingInterrupt, fail, logErrorCause, logInfo, scoped, succeed}
import zio.ZIOAspect.annotated

/**
 * Flink session mode cluster operator.
 */
trait FlinkSessionModeOperator extends FlinkClusterUnifyOperator {

  /**
   * Deploy Flink session cluster.
   */
  def deployCluster(spec: SessionClusterSpec): IO[DeploySessionClusterErr, Unit]

  /**
   * Submit job to Flink session cluster
   */
  def submitJob(spec: SessionJobSpec): IO[SubmitSessionJobErr, JobId]

}

object FlinkSessionModeOperator {

  type DeploySessionClusterErr = (ClusterAlreadyExist | ReviseClusterSpecErr | ResolvePodTemplateErr | ResolveLogConfigErr |
    ConvertToFlinkRawConfigErr | SubmitFlinkClusterFail | FlinkDataStoreErr | K8sFailure) with FlinkErr

  type SubmitSessionJobErr = (ClusterNotFound | NotSupportJobJarPath | DownloadRemoteJobJarErr | FlinkRestErr.RequestApiErr |
    FlinkRestErr.JarNotFound | FlinkDataStoreErr | K8sFailure) with FlinkErr
}

/**
 * Default implementation.
 */
class FlinkSessionModeOperatorImpl(
    flinkConf: FlinkConf,
    baseConf: BaseConf,
    fileServerConf: FileServerConf,
    k8sOperator: K8sOperator,
    fsBackendConf: FsBackendConf,
    remoteFs: RemoteFsOperator,
    observer: FlinkObserver)
    extends FlinkClusterUnifyOperatorImpl(flinkConf, k8sOperator, observer) with FlinkSessionModeOperator {

  private val podTemplateResolveLayer = flinkConf.asLayer ++ baseConf.asLayer ++ fileServerConf.asLayer
  private val flinkConfigConvertLayer = flinkConf.asLayer ++ fsBackendConf.asLayer

  /**
   * Deploy Flink session cluster.
   */
  // noinspection DuplicatedCode
  override def deployCluster(spec: SessionClusterSpec): IO[DeploySessionClusterErr, Unit] = {
    union[DeploySessionClusterErr, Unit] {
      for {
        fcid <- ZIO.succeed(spec.meta.fcid)

        // checking if fcid is occupied by remote k8s
        isAlive <- isRemoteClusterAlive(fcid)
        _       <- ZIO.fail(ClusterAlreadyExist(fcid)).when(isAlive)

        // revise cluster spec
        revisedSpec         <- ClusterSpecResolver.session.reviseSpec(spec)
        // resolve pod-template and log config
        podTemplateFilePath <- podTemplateFileOutputPath(fcid)
        logConfFilePath     <- logConfFileOutputPath(fcid)
        _                   <- PodTemplateResolver.resolvePodTemplateAndDump(revisedSpec, podTemplateFilePath).provideLayer(podTemplateResolveLayer)
        _                   <- LogConfigResolver.ensureFlinkLogsConfigFiles(logConfFilePath)
        // convert to raw flink config
        resolvedConfig      <- ClusterSpecResolver.session
                                 .resolveToFlinkConfig(revisedSpec, podTemplateFilePath, logConfFilePath)
                                 .provideLayer(flinkConfigConvertLayer)

        _ <- logInfo(s"Start to deploy flink session cluster:\n${resolvedConfig.dumpToMap(enableProtect = true).toPrettyStr}")

        // deploy cluster
        _ <- scoped {
               for
                 clusterClientFactory <- getFlinkClusterClientFactory(FlinkTargetType.K8sSession)
                 clusterSpecification <- attempt(clusterClientFactory.getClusterSpecification(resolvedConfig))
                 k8sClusterDescriptor <- usingAttempt(clusterClientFactory.createClusterDescriptor(resolvedConfig))
                 _                    <- attemptBlockingInterrupt(k8sClusterDescriptor.deploySessionCluster(clusterSpecification))
               yield ()
             }.mapError(SubmitFlinkClusterFail(fcid, FlinkTargetType.K8sSession, _))

        // tracking cluster
        _ <- observer.manager
               .track(fcid)
               .retryN(3)
               .tapErrorCause(cause => logErrorCause(s"Failed to submit flink cluster trace request, need to trace manually later.", cause))
               .ignore

        _ <- logInfo(s"Deploy flink session cluster successfully.")
      } yield ()
    }.tapErrorCause { cause => logErrorCause("Fail to deploy flink session cluster", cause).when(flinkConf.logFailedDeployReason) }
  } @@ annotated(spec.meta.fcid.toAnno*)

  /**
   * Submit job to Flink session cluster.
   */
  override def submitJob(spec: SessionJobSpec): IO[SubmitSessionJobErr, JobId] = {
    union[SubmitSessionJobErr, JobId] {
      for {
        // get rest api url of session cluster
        restUrl <- observer.restEndpoint
                     .getEnsure(spec.fcid)
                     .someOrFailUnion(ClusterNotFound(spec.fcid))
                     .map(_.chooseUrl)
        _       <- logInfo(s"Connect flink rest server for job submission: $restUrl")

        // download job jar from remote file system
        _          <- fail(NotSupportJobJarPath(spec.jobJar)).unless(paths.isPotaPath(spec.jobJar))
        _          <- logInfo(s"Downloading flink job jar from remote storage: ${spec.jobJar}")
        jobJarPath <- remoteFs
                        .download(
                          srcPath = spec.jobJar,
                          targetPath = s"${flinkConf.localTmpDeployDir}/${spec.namespace}@${spec.clusterId}/${paths.getFileName(spec.jobJar)}")
                        .mapBoth(DownloadRemoteJobJarErr(spec.jobJar, _), _.getPath)

        // submit job via flink rest api
        _          <- logInfo(s"Start to submit job to flink cluster: \n${spec.toPrettyStr}")
        _          <- logInfo(s"Uploading flink job jar to flink cluster, path: $jobJarPath, flink-rest: $restUrl")
        rest        = flinkRest(restUrl)
        jarId      <- rest.uploadJar(jobJarPath)
        jobId      <- rest.runJar(jarId, RunJobReq(spec))
        _          <- rest.deleteJar(jarId).ignore
        _          <- lfs.rm(jobJarPath).ignore

        _ <- logInfo(s"Submit job to flink session cluster successfully, jobId: $jobId")
      } yield jobId
    }.tapErrorCause { cause => logErrorCause("Fail to submit flink job to session cluster", cause).when(flinkConf.logFailedDeployReason) }
  } @@ annotated(spec.fcid.toAnno*)

}
