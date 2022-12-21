package potamoi.flink.operator

import com.devsisters.shardcake.Sharding
import potamoi.common.ScalaVersion.Scala212
import potamoi.common.Syntax.toPrettyString
import potamoi.errs.{headMessage, recurse}
import potamoi.flink.*
import potamoi.flink.model.*
import potamoi.flink.model.CheckpointStorageType.Filesystem
import potamoi.flink.model.FlinkExecMode.K8sSession
import potamoi.flink.model.FlK8sComponentName.jobmanager
import potamoi.flink.model.StateBackendType.Rocksdb
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.storage.FlinkSnapshotStorage
import potamoi.fs.S3Operator
import potamoi.kubernetes.{K8sConf, K8sOperator}
import potamoi.logger.PotaLogger
import potamoi.sharding.{ShardingConf, Shardings}
import potamoi.syntax.*
import potamoi.zios.*
import zio.{durationInt, IO, ZIO}
import zio.Console.printLine
import zio.Schedule.spaced

object FlinkOperatorTest {

  def testing[A](effect: (FlinkOperator, FlinkObserver) => IO[Throwable, A]): Unit = {
    val program =
      (
        for {
          opr <- ZIO.service[FlinkOperator]
          obr <- ZIO.service[FlinkObserver]
          _   <- obr.manager.registerEntities *> Sharding.registerScoped.ignore
          r   <- effect(opr, obr)
        } yield r
      ).tapErrorCause(cause => ZIO.logErrorCause(cause.recurse))

    ZIO
      .scoped(program)
      .provide(
        FlinkConfTest.asLayer,
        S3ConfTest.asLayer,
        K8sConfTest.asLayer,
        S3Operator.live,
        K8sOperator.live,
        FlinkSnapshotStorage.test,
        ShardingConf.test.asLayer,
        Shardings.test,
        FlinkObserver.live,
        FlinkOperator.live
      )
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
          clusterId = "session-01",
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
          clusterId = "app-t3",
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
//    obr.job.overview.list("app-t10" -> "fdev").repeatUntil(_.nonEmpty).debugPretty
    obr.job.overview.listAll.runCollect.watchPretty
  // todo bug
  }
  // deploy cluster with state backend
  @main def deployApplicationCluster3 = testing { (opr, obr) =>
    ???
  }

  // todo cancel job
  @main def cancelJob = testing { (opr, obr) =>
    ???
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
//  @main def cancelJob

// trigger savepoint

// stop job with savepoint

object FlinkSpecialOperationTest:

  // repeat submit
  // cancel and then restart
  // stop with savepoint and restart
  ???
