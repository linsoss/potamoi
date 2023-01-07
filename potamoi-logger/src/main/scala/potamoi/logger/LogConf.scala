package potamoi.logger

import zio.LogLevel
import zio.config.magnolia.name

/**
 * Logging configuration.
 */
case class LogConf(
    level: LogsLevel = LogsLevel.Debug,
    style: LogsStyle = LogsStyle.Plain,
    colored: Boolean = true,
    @name("in-one-line") inOneLine: Boolean = false)

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
