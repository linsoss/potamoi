package potamoi.fs

import potamoi.logger.PotaLogger
import potamoi.BaseConf
import potamoi.BaseConfDev.given
import potamoi.FsBackendConfDev.given
import potamoi.fs.backend.S3FsBackend
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
