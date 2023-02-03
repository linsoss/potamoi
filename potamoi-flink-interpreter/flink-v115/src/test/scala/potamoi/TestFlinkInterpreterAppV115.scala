package potamoi

import potamoi.akka.{AkkaMatrix, AkkaConf}
import potamoi.flink.{FlinkConf, FlinkMajorVer}
import potamoi.flink.interpreter.{FlinkInterpBootstrap, FlinkInterpConf}
import potamoi.fs.refactor.{FsBackendConf, RemoteFsOperator}
import potamoi.fs.refactor.backend.S3FsMirrorBackend
import potamoi.fs.S3FsBackendConfDev
import potamoi.logger.{LogConf, PotaLogger}
import potamoi.zios.asLayer
import zio.{Scope, ZLayer}

/**
 * Flink 1.16 sql interpreter app
 */
object TestFlinkInterpreterAppV115 extends FlinkInterpBootstrap(FlinkMajorVer.V115):

  override val bootstrap = PotaLogger.default

  override val run = program.provide(
    HoconConfig.empty,
    LogConf.default,
    BaseConf.test,
    S3FsBackendConfDev.asLayer,
    S3FsMirrorBackend.live,
    AkkaConf.localCluster(3315, List(3300, 3315), List(FlinkMajorVer.V115.nodeRole)),
    AkkaMatrix.live,
    FlinkConf.test,
    FlinkInterpConf.default,
    Scope.default
  )
