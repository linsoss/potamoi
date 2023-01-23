package potamoi.logger

import zio.{LogLevel, ZIO, ZLayer}
import zio.config.magnolia.{descriptor, name}
import zio.config.{read, ReadError}
import potamoi.configs

/**
 * Logging configuration.
 */
case class LogConf(
    level: LogsLevel = LogsLevel.Info,
    style: LogsStyle = LogsStyle.Plain,
    colored: Boolean = true,
    @name("in-one-line") inOneLine: Boolean = false)

object LogConf:

  def make: ZIO[Any, ReadError[String], LogConf] =
    for {
      _         <- ZIO.unit
      source     = configs.hoconSource("potamoi.log")
      configDesc = descriptor[LogConf].from(source)
      config    <- read(configDesc)
    } yield config

  lazy val live = ZLayer.fromZIO(make)

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
