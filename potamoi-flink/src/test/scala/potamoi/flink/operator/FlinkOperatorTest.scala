package potamoi.flink.operator

import com.devsisters.shardcake.Sharding
import potamoi.common.ScalaVersion.Scala212
import potamoi.common.Syntax.toPrettyString
import potamoi.errs.{headMessage, recurse}
import potamoi.flink.*
import potamoi.flink.model.{Fcid, FlinkRestSvcEndpoint, FlinkSessClusterDef, FlinkVersion}
import potamoi.flink.model.FlinkExecMode.K8sSession
import potamoi.flink.model.FlK8sComponentName.jobmanager
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

  // normal
  @main def deploySessionCluster = testing { (opr, obr) =>
    opr.session
      .deployCluster(
        FlinkSessClusterDef(
          flinkVer = FlinkVersion("1.15.2", Scala212),
          clusterId = "session-t6",
          namespace = "fdev",
          image = "flink:1.15.2"
        ))
      .debug
  }
