package potamoi.fs

import potamoi.fs.refactor.{FileServer, FileServerConf, RemoteFsOperator, S3FsBackendConf}
import potamoi.fs.refactor.backend.S3FsBackend
import potamoi.logger.PotaLogger
import potamoi.BaseConf
import potamoi.BaseConfDev.given
import potamoi.FsBackendConfDev.given
import zio.{ZIOAppDefault, ZLayer}
import zio.http.{Server, ServerConfig}
import zio.http.netty.server.NettyDriver

object FileServerTestApp extends ZIOAppDefault:

  override val bootstrap = PotaLogger.default

  val run = FileServer.run
    .provide(
      BaseConf.test,
      S3FsBackendConf.test,
      RemoteFsOperator.live,
      FileServerConf.default
    )
