package potamoi.fs

import potamoi.fs.refactor.{FileServer, FileServerConf}
import potamoi.fs.refactor.backend.S3FsBackend
import potamoi.logger.PotaLogger
import zio.{ZIOAppDefault, ZLayer}
import potamoi.zios.asLayer
import zio.http.{Server, ServerConfig}
import zio.http.netty.server.NettyDriver

object FileServerAppTest extends ZIOAppDefault:

  override val bootstrap = PotaLogger.default

  val run = FileServer.run
    .provide(
      S3FsBackendConfTest.asLayer,
      S3FsBackend.live,
      FileServerConf.test.asLayer
    )
