package potamoi

import potamoi.akka.{AkkaConf, AkkaMatrix}
import potamoi.flink.{FlinkConf, FlinkMajorVer}
import potamoi.flink.interpreter.{FlinkInterpBootstrap, FlinkInterpConf}
import potamoi.flink.storage.FlinkDataStorage
import potamoi.fs.refactor.{FsBackendConf, RemoteFsOperator}
import potamoi.fs.refactor.backend.S3FsMirrorBackend
import potamoi.fs.S3FsBackendConfDev
import potamoi.logger.{LogConf, LogsLevel, PotaLogger}
import potamoi.zios.asLayer
import zio.{Scope, ZLayer}

/**
 * Flink 1.16 sql interpreter app
 */
object TestFlinkInterpreterAppV116 extends FlinkInterpBootstrap(FlinkMajorVer.V116):

  override val bootstrap = PotaLogger.default

  override val run = program.provide(
    HoconConfig.empty,
    LogConf.default,
    BaseConf.test,
    S3FsBackendConfDev.asLayer,
    S3FsMirrorBackend.live,
    AkkaConf.localCluster(3316, List(3300, 3316), List(FlinkMajorVer.V116.nodeRole)),
    AkkaMatrix.live,
    FlinkConf.test,
    FlinkInterpConf.default,
    Scope.default
  )
