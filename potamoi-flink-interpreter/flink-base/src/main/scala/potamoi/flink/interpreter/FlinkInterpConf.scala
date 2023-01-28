package potamoi.flink.interpreter

import com.typesafe.config.Config
import potamoi.HoconConfig
import zio.ZLayer
import zio.config.magnolia.{descriptor, name}
import zio.config.read

/**
 * Flink interpreter app config.
 */
case class FlinkInterpConf(
    @name("http-port") httpPort: Int = 3500)

object FlinkInterpConf:

  val live: ZLayer[Config, Throwable, FlinkInterpConf] = ZLayer {
    for {
      source <- HoconConfig.hoconSource("potamoi.interpreter")
      config <- read(descriptor[FlinkInterpConf].from(source))
    } yield config
  }

  val default = ZLayer.succeed(FlinkInterpConf())
