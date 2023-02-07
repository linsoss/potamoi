package potamoi.flink.operator

import org.apache.flink.client.deployment.application.ApplicationConfiguration
import org.apache.flink.configuration.Configuration
import potamoi.flink.*
import potamoi.flink.model.deploy.{AppClusterSpec, JarAppClusterSpec, JobSavepointSpec, SqlAppClusterSpec}
import potamoi.BaseConf
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.FlinkRestRequest.{JobStatusInfo, StopJobSptReq, TriggerSptReq}
import potamoi.flink.model.snapshot.JobStates
import potamoi.flink.FlinkErr.*
import potamoi.flink.FlinkRestErr.{JobNotFound, RequestApiErr}
import potamoi.flink.operator.resolver.{ClusterSpecResolver, JarAppClusterSpecResolver, LogConfigResolver, PodTemplateResolver}
import potamoi.flink.FlinkConfigurationTool.dumpToMap
import potamoi.flink.model.{Fcid, Fjid, FlinkTargetType}
import potamoi.flink.ResolveFlinkClusterSpecErr.{ConvertToFlinkRawConfigErr, ResolveLogConfigErr, ResolvePodTemplateErr, ReviseClusterSpecErr}
import potamoi.flink.operator.FlinkClusterAppModeOperator.{AppModeJobOperationErr, DeployAppClusterErr}
import potamoi.fs.{FileServerConf, FsBackendConf, RemoteFsOperator}
import potamoi.kubernetes.K8sOperator
import potamoi.syntax.toPrettyStr
import potamoi.zios.*
import zio.{IO, ZIO}
import zio.ZIO.{attempt, attemptBlockingInterrupt, logErrorCause, logInfo, scoped, succeed}
import zio.ZIOAspect.annotated

/**
 * Flink application mode cluster operator.
 */
trait FlinkClusterAppModeOperator {

  /**
   * Deploy Flink application cluster.
   */
  def deployCluster(spec: AppClusterSpec): IO[DeployAppClusterErr, Unit]

  /**
   * Cancel job in flink application cluster.
   */
  def cancelJob(fcid: Fcid): IO[AppModeJobOperationErr, Unit]

  /**
   * Stop job in flink application cluster with savepoint.
   */
  def stopJob(fcid: Fcid, savepoint: JobSavepointSpec): IO[AppModeJobOperationErr, (Fjid, TriggerId)]

  /**
   * Triggers a savepoint of flink job.
   */
  def triggerJobSavepoint(fcid: Fcid, savepoint: JobSavepointSpec): IO[AppModeJobOperationErr, (Fjid, TriggerId)]

}

object FlinkClusterAppModeOperator {

  type DeployAppClusterErr = (JobAlreadyExist | ReviseClusterSpecErr | ResolvePodTemplateErr | ResolveLogConfigErr | ConvertToFlinkRawConfigErr |
    SubmitFlinkClusterFail | FlinkDataStoreErr | K8sFailure) with FlinkErr

  type AppModeJobOperationErr = (ClusterNotFound | EmptyJobOnCluster | JobNotFound | FlinkDataStoreErr | K8sFailure | RequestApiErr) with FlinkErr
}

class FlinkClusterAppModeOperatorImpl(
    flinkConf: FlinkConf,
    baseConf: BaseConf,
    fileServerConf: FileServerConf,
    k8sOperator: K8sOperator,
    fsBackendConf: FsBackendConf,
    observer: FlinkObserver)
    extends FlinkClusterUnifyOperatorImpl(flinkConf, k8sOperator, observer) with FlinkClusterAppModeOperator {

  private val podTemplateResolveLayer = flinkConf.asLayer ++ baseConf.asLayer ++ fileServerConf.asLayer
  private val flinkConfigConvertLayer = flinkConf.asLayer ++ fsBackendConf.asLayer

  /**
   * Deploy Flink application cluster.
   */
  // noinspection DuplicatedCode
  override def deployCluster(spec: AppClusterSpec): IO[DeployAppClusterErr, Unit] = {
    union[DeployAppClusterErr, Unit] {
      for {
        fcid <- ZIO.succeed(spec.meta.fcid)
        // ensure remote cluster is ready
        _    <- ensureRemoteEnvReady(fcid)

        //  revise cluster spec
        revisedSpec         <- spec match
                                 case sc: JarAppClusterSpec => ClusterSpecResolver.jarApp.reviseSpec(sc)
                                 case sc: SqlAppClusterSpec => ZIO.succeed(sc) // todo implement it
        // resolve pod-template and log config
        podTemplateFilePath <- podTemplateFileOutputPath(fcid)
        logConfFilePath     <- logConfFileOutputPath(fcid)
        _                   <- PodTemplateResolver.resolvePodTemplateAndDump(revisedSpec, podTemplateFilePath).provideLayer(podTemplateResolveLayer)
        _                   <- LogConfigResolver.ensureFlinkLogsConfigFiles(logConfFilePath)
        // convert to raw flink config
        resolvedConfig      <- (spec match
                                 case sc: JarAppClusterSpec => ClusterSpecResolver.jarApp.resolveToFlinkConfig(sc, podTemplateFilePath, logConfFilePath)
                                 case sc: SqlAppClusterSpec => ZIO.succeed(new Configuration())
                               ).provideLayer(flinkConfigConvertLayer)

        _ <- logInfo(s"Start to deploy flink session cluster:\n${resolvedConfig.dumpToMap(enableProtect = true).toPrettyStr}")

        // deploy app cluster
        _ <- scoped {
               for {
                 clusterClientFactory <- getFlinkClusterClientFactory(FlinkTargetType.K8sApplication)
                 clusterSpecification <- attempt(clusterClientFactory.getClusterSpecification(resolvedConfig))
                 appConfiguration     <- attempt(new ApplicationConfiguration(revisedSpec.appArgs.toArray, revisedSpec.appMain.orNull))
                 k8sClusterDescriptor <- usingAttempt(clusterClientFactory.createClusterDescriptor(resolvedConfig))
                 _                    <- attemptBlockingInterrupt(k8sClusterDescriptor.deployApplicationCluster(clusterSpecification, appConfiguration))
               } yield ()
             }.mapError(SubmitFlinkClusterFail(fcid, FlinkTargetType.K8sApplication, _))

        // tracking cluster
        _ <- observer.manager
               .track(fcid)
               .retryN(3)
               .tapErrorCause(cause => logErrorCause("Failed to submit flink cluster trace request, need to trace manually later", cause))
               .ignore

        _ <- logInfo(s"Deploy flink application cluster successfully.")
      } yield ()
    }.tapErrorCause { cause => logErrorCause("Fail to deploy flink application cluster", cause).when(flinkConf.logFailedDeployReason) }
  } @@ annotated(spec.meta.fcid.toAnno*)

  // Delete the flink cluster when no job exists in the flink cluster
  // or when the only one job existed is inactive.
  private def ensureRemoteEnvReady(fcid: Fcid): IO[JobAlreadyExist | FlinkDataStoreErr | K8sFailure, Unit] =
    isRemoteClusterAlive(fcid).flatMap {
      case false => ZIO.unit
      case true  =>
        observer.restEndpoint.getEnsure(fcid).flatMapUnion {
          case None      => clearCluster(fcid)
          case Some(ept) =>
            flinkRest(ept.chooseUrl).listJobsStatusInfo.map(_.headOption).catchAll(_ => ZIO.succeed(None)).flatMapUnion {
              case None                              => clearCluster(fcid)
              case Some(JobStatusInfo(jobId, state)) =>
                if !JobStates.isActive(state) then clearCluster(fcid)
                else ZIO.fail(JobAlreadyExist(Fjid(fcid, jobId), state))
            }
        }
    }

  private def clearCluster(fcid: Fcid): IO[FlinkDataStoreErr | K8sFailure, Unit] = {
    for
      _ <- logInfo(s"Deleting flink k8s resources...")
      _ <- internalKillCluster(fcid, wait = true)
             .catchAll {
               case _: ClusterNotFound                  => ZIO.unit
               case e: (FlinkDataStoreErr | K8sFailure) => ZIO.fail(e)
             }
      _ <- logInfo(s"Flink k8s resources deleted.")
    yield ()
  }

  /**
   * Cancel job in flink application cluster.
   */
  override def cancelJob(fcid: Fcid): IO[AppModeJobOperationErr, Unit] = {
    union[AppModeJobOperationErr, Unit] {
      for
        restUrl <- observer.restEndpoint.getEnsure(fcid).someOrFailUnion(ClusterNotFound(fcid)).map(_.chooseUrl)
        jobId   <- findFirstJobId(fcid, restUrl)
        _       <- flinkRest(restUrl).cancelJob(jobId)
      yield ()
    } @@ annotated(fcid.toAnno: _*)
  }

  /**
   * Stop job in flink application cluster with savepoint.
   */
  override def stopJob(fcid: Fcid, savepoint: JobSavepointSpec): IO[AppModeJobOperationErr, (Fjid, TriggerId)] = {
    union[AppModeJobOperationErr, (Fjid, TriggerId)] {
      for
        restUrl   <- observer.restEndpoint.getEnsure(fcid).someOrFailUnion(ClusterNotFound(fcid)).map(_.chooseUrl)
        jobId     <- findFirstJobId(fcid, restUrl)
        triggerId <- flinkRest(restUrl).stopJobWithSavepoint(jobId, StopJobSptReq(savepoint))
      yield Fjid(fcid, jobId) -> triggerId
    } @@ annotated(fcid.toAnno: _*)
  }

  /**
   * Triggers a savepoint of flink job.
   */
  override def triggerJobSavepoint(fcid: Fcid, savepoint: JobSavepointSpec): IO[AppModeJobOperationErr, (Fjid, TriggerId)] = {
    union[AppModeJobOperationErr, (Fjid, TriggerId)] {
      for
        restUrl   <- observer.restEndpoint.getEnsure(fcid).someOrFailUnion(ClusterNotFound(fcid)).map(_.chooseUrl)
        jobId     <- findFirstJobId(fcid, restUrl)
        triggerId <- flinkRest(restUrl).triggerSavepoint(jobId, TriggerSptReq(savepoint))
      yield Fjid(fcid, jobId) -> triggerId
    } @@ annotated(fcid.toAnno: _*)
  }

  private def findFirstJobId(fcid: Fcid, restUrl: String): IO[RequestApiErr | EmptyJobOnCluster, JobId] =
    flinkRest(restUrl).listJobsStatusInfo
      .map(_.headOption.map(_.id))
      .someOrFailUnion(EmptyJobOnCluster(fcid))
}
