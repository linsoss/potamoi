package potamoi.flink.operator

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.DoNotDiscover
import potamoi.*
import potamoi.common.ScalaVersion.Scala212
import potamoi.common.Syntax.toPrettyString
import potamoi.debugs.*
import potamoi.flink.*
import potamoi.flink.model.*
import potamoi.flink.model.deploy.CheckpointStorageType.Filesystem
import potamoi.flink.model.FlinkTargetType.K8sSession
import potamoi.flink.model.deploy.*
import potamoi.flink.model.deploy.StateBackendType.Rocksdb
import potamoi.flink.model.snapshot.{JobState, JobStates}
import potamoi.flink.model.snapshot.FlK8sComponentName.jobmanager
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.storage.FlinkDataStorage
import potamoi.kubernetes.{K8sConf, K8sOperator}
import potamoi.logger.{LogConf, PotaLogger}
import potamoi.syntax.*
import potamoi.zios.*
import potamoi.BaseConfDev.given
import potamoi.FsBackendConfDev.given
import potamoi.akka.{AkkaConf, AkkaMatrix}
import potamoi.fs.{FileServer, FileServerConf, RemoteFsOperator}
import zio.{durationInt, IO, Scope, ZIO}
import zio.Console.printLine
import zio.Schedule.spaced
import zio.ZIO.logInfo

object FlinkOperatorTest:

  def testing[E, A](effect: (FlinkOperator, FlinkObserver) => IO[E, A]): Unit = {
    val program =
      (
        for {
          _   <- AkkaMatrix.active
          _   <- FlinkObserver.active
          opr <- ZIO.service[FlinkOperator]
          obr <- ZIO.service[FlinkObserver]
          _   <- FileServer.run.forkDaemon
          r   <- effect(opr, obr)
          _   <- ZIO.never
        } yield r
      ).tapErrorCause(cause => ZIO.logErrorCause(cause))

    program
      .provide(
        HoconConfig.empty,
        LogConf.default,
        BaseConf.test,
        S3FsBackendConfDev.test,
        RemoteFsOperator.live,
        FileServerConf.default,
        FlinkConf.test,
        K8sConf.default,
        K8sOperator.live,
        FlinkDataStorage.test,
        FlinkObserver.live,
        FlinkOperator.live,
        AkkaConf.local(List(NodeRoles.flinkService)),
        AkkaMatrix.live,
        Scope.default)
      .provideLayer(PotaLogger.default)
      .run
      .exitCode
  }

import potamoi.flink.operator.FlinkOperatorTest.*

// ------------------------------- test start -------------------------------------

@DoNotDiscover
class FlinkSessClusterOperatorTest extends AnyWordSpec:

  "deploy simple session cluster" in testing { (opr, obr) =>
    opr.session
      .deployCluster(
        SessionClusterSpec(
          meta = ClusterMeta(
            flinkVer = "1.16.1" -> Scala212,
            clusterId = "session-t6",
            namespace = "fdev",
            image = "flink:1.16.1"
          )
        ))
      .debug
  }

  "deploy simple session cluster with state backend" in testing { (opr, obr) =>
    opr.session
      .deployCluster(
        SessionClusterSpec(
          meta = ClusterMeta(
            flinkVer = "1.16.1" -> Scala212,
            clusterId = "session-t6",
            namespace = "fdev",
            image = "flink:1.16.1"
          ),
          props = ClusterProps(
            stateBackend = StateBackendProp(
              backendType = Rocksdb,
              checkpointStorage = Filesystem,
              checkpointDir = "s3://flink-dev/checkpoints",
              savepointDir = "s3://flink-dev/savepoints",
              incremental = true
            ))
        ))
      .debug
  }

  "deploy session cluster with state backend and extra dependencies" in testing { (opr, obr) =>
    opr.session
      .deployCluster(
        SessionClusterSpec(
          meta = ClusterMeta(
            flinkVer = "1.16.1" -> Scala212,
            clusterId = "session-t3",
            namespace = "fdev",
            image = "flink:1.16.1"
          ),
          props = ClusterProps(
            stateBackend = StateBackendProp(
              backendType = Rocksdb,
              checkpointStorage = Filesystem,
              checkpointDir = "s3://flink-dev/checkpoints",
              savepointDir = "s3://flink-dev/savepoints",
              incremental = true
            ),
            udfJars = Set(
              "pota://flink-connector-jdbc-1.16.1.jar",
              "pota://mysql-connector-java-8.0.30.jar"
            )
          )
        ))
      .debug *>
    obr.cluster.overview.get("session-t3" -> "fdev").repeatUntil(_.isDefined).debugPretty
  }

  "submit job to session cluster" in testing { (opr, obr) =>
    opr.session
      .submitJob(
        SessionJobSpec(
          clusterId = "session-t6",
          namespace = "fdev",
          jobJar = "pota://flink-1.16.0/examples/streaming/StateMachineExample.jar"
        ))
      .debug
  }

@DoNotDiscover
class FlinkAppClusterOperatorTest extends AnyWordSpec:

  "deploy simple application cluster" in testing { (opr, obr) =>
    opr.application
      .deployCluster(
        JarAppClusterSpec(
          meta = ClusterMeta(
            flinkVer = "1.16.1" -> Scala212,
            clusterId = "app-t10",
            namespace = "fdev",
            image = "flink:1.16.1",
          ),
          jobJar = "local:///opt/flink/examples/streaming/StateMachineExample.jar"
        ))
      .debug *>
    obr.job.overview.list("app-t10" -> "fdev").repeatUntil(_.nonEmpty).debugPretty
  }

  "deploy cluster with user jar on remote fs" in testing { (opr, obr) =>
    opr.application
      .deployCluster(
        JarAppClusterSpec(
          meta = ClusterMeta(
            flinkVer = "1.16.1" -> Scala212,
            clusterId = "app-t10",
            namespace = "fdev",
            image = "flink:1.16.1",
          ),
          jobJar = "pota://flink-1.16.0/examples/streaming/StateMachineExample.jar"
        ))
      .debug
  }

  "deploy application cluster with state backend" in testing { (opr, obr) =>
    opr.application
      .deployCluster(
        JarAppClusterSpec(
          meta = ClusterMeta(
            flinkVer = "1.16.1" -> Scala212,
            clusterId = "app-t10",
            namespace = "fdev",
            image = "flink:1.16.1",
          ),
          props = ClusterProps(
            stateBackend = StateBackendProp(
              backendType = Rocksdb,
              checkpointStorage = Filesystem,
              checkpointDir = "s3://flink-dev/checkpoints",
              savepointDir = "s3://flink-dev/savepoints",
              incremental = true
            )
          ),
          jobJar = "pota://flink-1.16.0/examples/streaming/StateMachineExample.jar",
        ))
      .debug
  }

  // todo test
  "cancel job" in testing { (opr, obr) =>
    obr.manager.track("app-t10" -> "fdev") *>
    obr.job.overview.listJobId("app-t10" -> "fdev").repeatUntil(_.nonEmpty) *>
    opr.application.cancelJob("app-t10" -> "fdev") *>
    obr.job.overview.listJobState("app-t10" -> "fdev").watchPretty
  }

  private val appSpec = JarAppClusterSpec(
    meta = ClusterMeta(
      flinkVer = "1.16.1" -> Scala212,
      clusterId = "app-t10",
      namespace = "fdev",
      image = "flink:1.16.1",
    ),
    jobJar = "pota://flink-1.16.0/examples/streaming/StateMachineExample.jar",
    props = ClusterProps(
      stateBackend = StateBackendProp(
        backendType = Rocksdb,
        checkpointStorage = Filesystem,
        checkpointDir = "s3://flink-dev/checkpoints",
        savepointDir = "s3://flink-dev/savepoints",
        incremental = true
      )
    )
  )

  "recover from savepoint" in testing { (opr, obr) =>
    for {
      _ <- opr.application.deployCluster(appSpec)
      _ <- obr.job.overview
             .listJobState("app-t10" -> "fdev")
             .distPollStream(500.millis)
             .takeUntil(_.values.headOption.contains(JobState.RUNNING))
             .runForeach(e => printLine(e.toPrettyStr))
      _ <- ZIO.sleep(20.seconds)

      _       <- logInfo("Let's stop job !")
      trigger <- opr.application.stopJob("app-t10" -> "fdev", JobSavepointSpec(savepointPath = "s3p://flink-dev/savepoints/myjob"))
      _       <- obr.job.savepointTrigger
                   .watch(trigger._1, trigger._2)
                   .takeUntil(e => e.isCompleted)
                   .runForeach(e => printLine(e.toPrettyStr))

      _ <- obr.job.overview
             .getJobState(trigger._1)
             .distPollStream(500.millis)
             .takeUntil(e => e.exists(s => !JobStates.isActive(s)))
             .runForeach(e => printLine(e.toPrettyStr))
      _ <- ZIO.sleep(3.seconds)

      _ <- logInfo("Let's recover job !")
      _ <- opr.application.deployCluster(
             appSpec.copy(restore = SavepointRestoreProp(savepointPath = "s3p://flink-dev/savepoints/myjob"))
           )
      _ <- obr.job.overview.listJobState("app-t10" -> "fdev").watchPretty
    } yield ()
  }

@DoNotDiscover
object FlinkClusterUnifyOperatorTest extends AnyWordSpec:

  "force kill cluster" in testing { (opr, obr) =>
    opr.unify.killCluster("session-t6" -> "fdev")
  }

  "kill cluster and observe" in testing { (opr, obr) =>
    obr.manager.track("session-t6" -> "fdev") *>
    obr.cluster.overview.get("session-t6" -> "fdev").repeatUntil(_.isDefined) *>
    opr.unify.killCluster("session-t6" -> "fdev") *>
    obr.manager.isBeTracked("session-t6" -> "fdev") *>
    obr.cluster.overview.get("session-t6" -> "fdev").debugPretty
  }

  "cancel job" in testing { (opr, obr) =>
    for {
      _   <- obr.manager.track("app-t10" -> "fdev")
      jid <- obr.job.overview.listJobId("app-t10" -> "fdev").repeatUntil(_.nonEmpty).map(_.head)
      _   <- obr.job.overview.listJobState("app-t10" -> "fdev").watchPretty.fork
      _   <- opr.unify.cancelJob(jid)
      _   <- ZIO.never
    } yield ()
  }

  "trigger savepoint" in testing { (opr, obr) =>
    for {
      _         <- obr.manager.track("app-t10" -> "fdev")
      jid       <- obr.job.overview.listJobId("app-t10" -> "fdev").repeatUntil(_.nonEmpty).map(_.head)
      triggerId <- opr.unify.triggerJobSavepoint(jid, JobSavepointSpec()).map(_._2)
      _         <- obr.job.savepointTrigger.watch(jid, triggerId).runForeach(s => printLine(s.toPrettyStr))
    } yield ()
  }

  "stop job with savepoint" in testing { (opr, obr) =>
    for {
      _         <- obr.manager.track("app-t10" -> "fdev")
      jid       <- obr.job.overview.listJobId("app-t10" -> "fdev").repeatUntil(_.nonEmpty).map(_.head)
      triggerId <- opr.unify.stopJob(jid, JobSavepointSpec()).map(_._2)
      _         <- obr.job.savepointTrigger.watch(jid, triggerId).runForeach(s => printLine(s.toPrettyStr))
    } yield ()
  }
