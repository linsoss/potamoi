package potamoi.fs.refactor

import com.typesafe.config.Config
import potamoi.common.HoconConfig
import zio.{ULayer, ZIO, ZLayer}
import zio.config.magnolia.{descriptor, name}
import zio.config.read
import zio.http.ServerConfig

/**
 * Potamoi remote file storage server app configuration.
 */
case class FileServerConf(
    @name("host") host: String,
    @name("port") port: Int = 3520)

object FileServerConf:

  val live: ZLayer[Config, Throwable, FileServerConf] = ZLayer {
    for {
      source <- HoconConfig.hoconSource("potamoi.fs-server")
      config <- read(descriptor[FileServerConf].from(source))
    } yield config
  }

  val default: ULayer[FileServerConf] = ZLayer.succeed(FileServerConf("127.0.0.1", 3520))
