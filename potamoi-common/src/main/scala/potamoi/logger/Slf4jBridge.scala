package potamoi.logger

import org.slf4j.impl.ZIOLoggerFactory
import zio.{LogLevel, ZIO, ZLayer}

/**
 * Bridge for translating SLF4J logging to ZIO logging.
 */
object Slf4jBridge {

  def initialize(logLevel: LogLevel = LogLevel.Info, mdcKeys: Vector[String] = Vector.empty): ZLayer[Any, Nothing, Unit] =
    ZLayer {
      ZIO.runtime[Any].flatMap { runtime =>
        ZIO.succeed(ZIOLoggerFactory.initialize(runtime, logLevel, mdcKeys))
      }
    }

  val loggerNameAnno = "@loggerName"
  val threadNameAnno = "@threadName"
}
