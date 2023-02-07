package potamoi

import potamoi.akka.{AkkaConf, AkkaMatrix}
import potamoi.flink.{FlinkConf, FlinkMajorVer}
import potamoi.flink.interpreter.{FlinkInterpBootstrap, FlinkInterpConf}
import potamoi.FsBackendConfDev.given
import potamoi.logger.{LogConf, PotaLogger}
import potamoi.BaseConfDev.given
import potamoi.fs.backend.S3FsMirrorBackend
import potamoi.fs.{FsBackendConf, RemoteFsOperator, S3FsBackendConf}
import zio.{Scope, ZLayer}

/**
 * Flink 1.16 sql interpreter app
 */
object FlinkInterpreterV115TestApp extends FlinkInterpBootstrap(FlinkMajorVer.V115):

  override val bootstrap = PotaLogger.default

  override val run = program.provide(
    HoconConfig.empty,
    LogConf.default,
    BaseConf.test,
    S3FsBackendConf.test,
    RemoteFsOperator.live,
    AkkaConf.localCluster(3315, List(3300, 3315), List(FlinkMajorVer.V115.nodeRole)),
    AkkaMatrix.live,
    FlinkConf.test,
    FlinkInterpConf.default,
    Scope.default
  )
