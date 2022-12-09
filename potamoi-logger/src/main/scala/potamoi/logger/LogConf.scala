package potamoi.logger

import zio.LogLevel
import zio.config.magnolia.name

/**
 * Logging configuration.
 */
case class LogConf(
    level: LogsLevel = LogsLevel.info,
    style: LogsStyle = LogsStyle.plain,
    colored: Boolean = true,
    @name("in-one-line") inOneLine: Boolean = false)

/**
 * Potamoi logging line style.
 */
enum LogsStyle:
  case plain
  case json

/**
 * Logging level.
 */
enum LogsLevel(val zioLevel: LogLevel):
  case trace   extends LogsLevel(LogLevel.Trace)
  case debug   extends LogsLevel(LogLevel.Debug)
  case info    extends LogsLevel(LogLevel.Info)
  case warning extends LogsLevel(LogLevel.Warning)
  case error   extends LogsLevel(LogLevel.Error)
  case fatal   extends LogsLevel(LogLevel.Fatal)
