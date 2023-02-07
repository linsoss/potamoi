package potamoi

import potamoi.akka.{AkkaConf, AkkaMatrix}
import potamoi.flink.{FlinkConf, FlinkMajorVer}
import potamoi.flink.interpreter.{FlinkInterpBootstrap, FlinkInterpConf}
import potamoi.flink.storage.FlinkDataStorage
import potamoi.logger.{LogConf, LogsLevel, PotaLogger}
import potamoi.BaseConfDev.given
import potamoi.FsBackendConfDev.given
import potamoi.fs.backend.S3FsMirrorBackend
import potamoi.fs.{FsBackendConf, RemoteFsOperator, S3FsBackendConf}
import zio.{Scope, ZLayer}

/**
 * Flink 1.16 sql interpreter app
 */
object FlinkInterpreterV116TestApp extends FlinkInterpBootstrap(FlinkMajorVer.V116):

  override val bootstrap = PotaLogger.default

  override val run = program.provide(
    HoconConfig.empty,
    LogConf.default,
    BaseConf.test,
    S3FsBackendConf.test,
    RemoteFsOperator.live,
    AkkaConf.localCluster(3316, List(3300, 3316), List(FlinkMajorVer.V116.nodeRole)),
    AkkaMatrix.live,
    FlinkConf.test,
    FlinkInterpConf.default,
    Scope.default
  )
