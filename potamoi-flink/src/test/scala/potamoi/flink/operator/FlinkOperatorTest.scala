package potamoi.flink.operator

import potamoi.{BaseConf, HoconConfig, NodeRoles, PotaErr}
import potamoi.akka.{AkkaMatrix, AkkaConf}
import potamoi.common.ScalaVersion.Scala212
import potamoi.common.Syntax.toPrettyString
import potamoi.debugs.*
import potamoi.flink.*
import potamoi.flink.model.*
import potamoi.flink.model.CheckpointStorageType.Filesystem
import potamoi.flink.model.FlinkTargetType.K8sSession
import potamoi.flink.model.FlK8sComponentName.jobmanager
import potamoi.flink.model.StateBackendType.Rocksdb
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.storage.FlinkDataStorage
import potamoi.fs.S3Operator
import potamoi.kubernetes.{K8sConf, K8sOperator}
import potamoi.logger.{LogConf, PotaLogger}
import potamoi.syntax.*
import potamoi.zios.*
import zio.{durationInt, IO, Scope, ZIO}
import zio.Console.printLine
import zio.Schedule.spaced
import zio.ZIO.logInfo

object FlinkOperatorTest {

  def testing[E, A](effect: (FlinkOperator, FlinkObserver) => IO[E, A]): Unit = {
    val program =
      (
        for {
          opr <- ZIO.service[FlinkOperator]
          obr <- ZIO.service[FlinkObserver]
          r   <- effect(opr, obr)
        } yield r
      ).tapErrorCause(cause => ZIO.logErrorCause(cause))

    program
      .provide(
        HoconConfig.empty,
        S3ConfDev.asLayer, // todo remove
        LogConf.default,
        BaseConf.test,
        FlinkConf.test,
        K8sConf.default,
        S3Operator.live,   // todo replace with RemoteFsOperator
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
}

import potamoi.flink.operator.FlinkOperatorTest.*

object FlinkSessClusterOperatorTest:

  // deploy simple cluster
  @main def deploySessionCluster = testing { (opr, obr) =>
    opr.session
      .deployCluster(
        FlinkSessClusterDef(
          flinkVer = "1.15.2" -> Scala212,
          clusterId = "session-t6",
          namespace = "fdev",
          image = "flink:1.15.2"
        ))
      .debug
  }

  // deploy cluster with state backend
  @main def deploySessionCluster2 = testing { (opr, obr) =>
    opr.session
      .deployCluster(
        FlinkSessClusterDef(
          flinkVer = "1.15.2" -> Scala212,
          clusterId = "session-t6",
          namespace = "fdev",
          image = "flink:1.15.2",
          stateBackend = StateBackendConfig(
            backendType = Rocksdb,
            checkpointStorage = Filesystem,
            checkpointDir = "s3://flink-dev/checkpoints",
            savepointDir = "s3://flink-dev/savepoints",
            incremental = true
          )
        ))
      .debug
  }

  // deploy cluster with state backend and extra dependencies
  @main def deploySessionCluster3 = testing { (opr, obr) =>
    opr.session
      .deployCluster(
        FlinkSessClusterDef(
          flinkVer = "1.15.2" -> Scala212,
          clusterId = "session-t3",
          namespace = "fdev",
          image = "flink:1.15.2",
          stateBackend = StateBackendConfig(
            backendType = Rocksdb,
            checkpointStorage = Filesystem,
            checkpointDir = "s3://flink-dev/checkpoints",
            savepointDir = "s3://flink-dev/savepoints",
            incremental = true
          ),
          injectedDeps = Set(
            "s3://flink-dev/flink-connector-jdbc-1.15.2.jar",
            "s3://flink-dev/mysql-connector-java-8.0.30.jar"
          )
        ))
      .debug *>
    obr.cluster.overview.get("session-t3" -> "fdev").repeatUntil(_.isDefined).debugPretty
  }

  // submit job to session cluster
  @main def submitJobToSessionCluster = testing { (opr, obr) =>
    opr.session
      .submitJob(
        FlinkSessJobDef(
          clusterId = "session-t6",
          namespace = "fdev",
          jobJar = "s3://flink-dev/flink-1.15.2/example/streaming/StateMachineExample.jar"
        ))
      .debug
  }

object FlinkAppClusterOperatorTest:

  // deploy simple cluster
  @main def deployApplicationCluster = testing { (opr, obr) =>
    opr.application
      .deployCluster(
        FlinkAppClusterDef(
          flinkVer = "1.15.2" -> Scala212,
          clusterId = "app-t10",
          namespace = "fdev",
          image = "flink:1.15.2",
          jobJar = "local:///opt/flink/examples/streaming/StateMachineExample.jar"
        ))
      .debug *>
    obr.job.overview.list("app-t10" -> "fdev").repeatUntil(_.nonEmpty).debugPretty
  }

  // deploy cluster with user jar on s3
  @main def deployApplicationCluster2 = testing { (opr, obr) =>
    opr.application
      .deployCluster(
        FlinkAppClusterDef(
          flinkVer = "1.15.2" -> Scala212,
          clusterId = "app-t10",
          namespace = "fdev",
          image = "flink:1.15.2",
          jobJar = "s3://flink-dev/flink-1.15.2/example/streaming/StateMachineExample.jar"
        ))
      .debug
  }

  // deploy cluster with state backend
  @main def deployApplicationCluster3 = testing { (opr, obr) =>
    opr.application
      .deployCluster(
        FlinkAppClusterDef(
          flinkVer = "1.15.2" -> Scala212,
          clusterId = "app-t10",
          namespace = "fdev",
          image = "flink:1.15.2",
          jobJar = "s3://flink-dev/flink-1.15.2/example/streaming/StateMachineExample.jar",
          stateBackend = StateBackendConfig(
            backendType = Rocksdb,
            checkpointStorage = Filesystem,
            checkpointDir = "s3://flink-dev/checkpoints",
            savepointDir = "s3://flink-dev/savepoints",
            incremental = true
          )
        ))
      .debug *>
    obr.job.overview.list("app-t10" -> "fdev").repeatUntil(_.nonEmpty).debugPretty
  }

  //  cancel job
  @main def cancelApplicationJob = testing { (opr, obr) =>
    obr.manager.track("app-t10" -> "fdev") *>
    obr.job.overview.listJobId("app-t10" -> "fdev").repeatUntil(_.nonEmpty) *>
    opr.application.cancelJob("app-t10" -> "fdev") *>
    obr.job.overview.listJobState("app-t10" -> "fdev").watchPretty
  }

  private val appDef = FlinkAppClusterDef(
    flinkVer = "1.15.2" -> Scala212,
    clusterId = "app-t10",
    namespace = "fdev",
    image = "flink:1.15.2",
    jobJar = "s3://flink-dev/flink-1.15.2/example/streaming/StateMachineExample.jar",
    stateBackend = StateBackendConfig(
      backendType = Rocksdb,
      checkpointStorage = Filesystem,
      checkpointDir = "s3://flink-dev/checkpoints",
      savepointDir = "s3://flink-dev/savepoints",
      incremental = true
    )
  )

  // stop with savepoint and restart
  @main def recoverFromSavepoint = testing { (opr, obr) =>
    for {
      _ <- opr.application.deployCluster(appDef)
      _ <- obr.job.overview
             .listJobState("app-t10" -> "fdev")
             .distPollStream(500.millis)
             .takeUntil(_.values.headOption.contains(JobState.RUNNING))
             .runForeach(e => printLine(e.toPrettyStr))
      _ <- ZIO.sleep(20.seconds)

      _       <- logInfo("Let's stop job !")
      trigger <- opr.application.stopJob("app-t10" -> "fdev", FlinkJobSavepointDef(savepointPath = "s3p://flink-dev/savepoints/myjob"))
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
             appDef.copy(restore = SavepointRestoreConfig(savepointPath = "s3p://flink-dev/savepoints/myjob"))
           )
      _ <- obr.job.overview.listJobState("app-t10" -> "fdev").watchPretty
    } yield ()
  }

object FlinkClusterUnifyOperatorTest:

  // kill cluster
  @main def testForceKillCluster = testing { (opr, obr) =>
    opr.unify.killCluster("session-t6" -> "fdev")
  }

  // kill cluster
  @main def testKillClusterAndObserve = testing { (opr, obr) =>
    obr.manager.track("session-t6" -> "fdev") *>
    obr.cluster.overview.get("session-t6" -> "fdev").repeatUntil(_.isDefined) *>
    opr.unify.killCluster("session-t6" -> "fdev") *>
    obr.manager.isBeTracked("session-t6" -> "fdev") *>
    obr.cluster.overview.get("session-t6" -> "fdev").debugPretty
  }

  // cancel job
  @main def cancelNormalJob = testing { (opr, obr) =>
    for {
      _   <- obr.manager.track("app-t10" -> "fdev")
      jid <- obr.job.overview.listJobId("app-t10" -> "fdev").repeatUntil(_.nonEmpty).map(_.head)
      _   <- obr.job.overview.listJobState("app-t10" -> "fdev").watchPretty.fork
      _   <- opr.unify.cancelJob(jid)
      _   <- ZIO.never
    } yield ()
  }

  // trigger savepoint
  @main def triggerSavepoint = testing { (opr, obr) =>
    for {
      _         <- obr.manager.track("app-t10" -> "fdev")
      jid       <- obr.job.overview.listJobId("app-t10" -> "fdev").repeatUntil(_.nonEmpty).map(_.head)
      triggerId <- opr.unify.triggerJobSavepoint(jid, FlinkJobSavepointDef()).map(_._2)
      _         <- obr.job.savepointTrigger.watch(jid, triggerId).runForeach(s => printLine(s.toPrettyStr))
    } yield ()
  }

  // stop job with savepoint
  @main def stopJobWithSavepoint = testing { (opr, obr) =>
    for {
      _         <- obr.manager.track("app-t10" -> "fdev")
      jid       <- obr.job.overview.listJobId("app-t10" -> "fdev").repeatUntil(_.nonEmpty).map(_.head)
      triggerId <- opr.unify.stopJob(jid, FlinkJobSavepointDef()).map(_._2)
      _         <- obr.job.savepointTrigger.watch(jid, triggerId).runForeach(s => printLine(s.toPrettyStr))
    } yield ()
  }
