package potamoi

import com.typesafe.config.Config
import zio.{ULayer, ZIO, ZLayer}
import zio.config.{read, ReadError}
import zio.config.magnolia.{descriptor, name}

/**
 * Potamoi basic config
 */
case class BaseConf(@name("data-dir") dataDir: String = "/var/potamoi")

object BaseConf:

  val live: ZLayer[Config, Throwable, BaseConf] = ZLayer {
    for {
      source <- HoconConfig.hoconSource("potamoi.base")
      config <- read(descriptor[BaseConf].from(source))
    } yield config
  }

  val test: ULayer[BaseConf] = ZLayer.succeed(BaseConf(dataDir = "var/potamoi"))
