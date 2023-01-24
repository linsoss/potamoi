package potamoi

import com.devsisters.shardcake.Sharding
import potamoi.flink.FlinkMajorVer
import potamoi.flink.interpreter.{FlinkInterpBootstrap, FlinkInterpConf, FlinkSqlInterpreter}
import potamoi.fs.refactor.{FsBackendConf, RemoteFsOperator}
import potamoi.fs.refactor.backend.S3FsMirrorBackend
import potamoi.fs.S3FsBackendConfDev
import potamoi.logger.PotaLogger
import potamoi.sharding.{ShardingConf, Shardings}
import potamoi.zios.asLayer
import zio.{Scope, ZLayer}

/**
 * Flink 1.16 sql interpreter app
 */
object TestFlinkInterpreterAppV116 extends FlinkInterpBootstrap(FlinkMajorVer.V116):

  override val bootstrap = PotaLogger.default

  override val run = active.provide(
    FlinkInterpConf.default,
    S3FsBackendConfDev.asLayer,
    S3FsMirrorBackend.live,
    ShardingConf.test.project(_.copy(selfPort = 3416)),
    Shardings.test,
    FlinkSqlInterpreter.live(shardEntity),
    Scope.default
  )
