package potamoi.logger

import potamoi.syntax.toPrettyStr
import potamoi.HoconConfig
import zio.{Console, LogLevel, UIO, ULayer, ZIO, ZLayer}
import zio.config.magnolia.{descriptor, name}
import zio.config.read

/**
 * Logging configuration.
 */
case class LogConf(
    level: LogsLevel = LogsLevel.Info,
    style: LogsStyle = LogsStyle.Plain,
    colored: Boolean = true,
    @name("in-one-line") inOneLine: Boolean = false)

object LogConf:

  def make: UIO[LogConf] = {
    val find = for {
      source <- HoconConfig.directHoconSource("potamoi.log")
      config <- read(descriptor[LogConf].from(source))
    } yield config
    // using default config
    find.catchAll(_ => ZIO.succeed(LogConf()))
  }.tap { conf =>
    Console.printLine(s"PotaLogger config: ${conf.toPrettyStr}").ignore
  }

  lazy val live: ULayer[LogConf] = ZLayer.fromZIO(make)

  lazy val default: ULayer[LogConf] = ZLayer.succeed(LogConf())

/**
 * Potamoi logging line style.
 */
enum LogsStyle:
  case Plain
  case Json

/**
 * Logging level.
 */
enum LogsLevel(val zioLevel: LogLevel):
  case Trace   extends LogsLevel(LogLevel.Trace)
  case Debug   extends LogsLevel(LogLevel.Debug)
  case Info    extends LogsLevel(LogLevel.Info)
  case Warning extends LogsLevel(LogLevel.Warning)
  case Error   extends LogsLevel(LogLevel.Error)
  case Fatal   extends LogsLevel(LogLevel.Fatal)
