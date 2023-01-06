package potamoi.fs.refactor

import zio.config.magnolia.name
import zio.http.ServerConfig
import zio.json.JsonCodec
import zio.{ZIO, ZLayer}

/**
 * Potamoi remote file storage server app configuration.
 */
case class FileServerConf(
    @name("host") host: String,
    @name("port") port: Int = 3400)
    derives JsonCodec

object FileServerConf:
  val test = FileServerConf("127.0.0.1", 3400)
