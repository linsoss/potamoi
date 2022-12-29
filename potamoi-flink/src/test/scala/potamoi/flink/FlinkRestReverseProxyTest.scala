package potamoi.flink

import com.devsisters.shardcake
import com.devsisters.shardcake.Sharding
import potamoi.common.ScalaVersion.Scala212
import potamoi.common.Syntax.toPrettyString
import potamoi.errs.{headMessage, recurse}
import potamoi.flink
import potamoi.flink.*
import potamoi.flink.model.*
import potamoi.flink.model.CheckpointStorageType.Filesystem
import potamoi.flink.model.FlinkExecMode.K8sSession
import potamoi.flink.model.FlK8sComponentName.jobmanager
import potamoi.flink.model.StateBackendType.Rocksdb
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.operator.FlinkOperator
import potamoi.flink.storage.FlinkSnapshotStorage
import potamoi.fs.S3Operator
import potamoi.kubernetes.{K8sConf, K8sOperator}
import potamoi.logger.PotaLogger
import potamoi.sharding.*
import potamoi.sharding.LocalShardManager.withLocalShardManager
import potamoi.syntax.*
import potamoi.zios.*
import zio.*
import zio.Console.printLine
import zio.Schedule.spaced
import zio.ZIO.logInfo
import zio.http.*
import zio.http.model.Status

object FlinkRestReverseProxyTestApp extends ZIOAppDefault:

  override val bootstrap = PotaLogger.default

  val program = {
    for {
      opr <- ZIO.service[FlinkOperator]
      obr <- ZIO.service[FlinkObserver]
      _   <- obr.manager.registerEntities *> Sharding.registerScoped.ignore
      _   <- obr.manager.track("app-t1" -> "fdev")
      _   <- obr.manager.track("app-t2" -> "fdev")
      _   <- opr.restProxy.enable("app-t1" -> "fdev")
      _   <- opr.restProxy.enable("app-t2" -> "fdev")
    } yield ()
  } *> Server.serve(FlinkRestReverseProxy.route)

  val run = program
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
      FlinkOperator.live,
      FlinkRestProxyProvider.live,
      Server.default,
      Client.default,
      Scope.default
    )
    .withLocalShardManager
