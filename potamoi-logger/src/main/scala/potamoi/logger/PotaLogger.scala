package potamoi.logger

import zio.{LogLevel, RLayer, Trace, ULayer, URLayer, ZIO, ZLayer}
import zio.logging.*
import zio.logging.LogFormat.*

import java.time.format.DateTimeFormatter

/**
 * Potamoi ZIO logger layer.
 * Automatic transfer of slf4j logs to zio-logging, and support for automatic extraction
 * of original slf4j MDC, thread name and logger name info to zio-logging annotations.
 */
object PotaLogger {

  lazy val live: URLayer[LogConf, Unit] = ZLayer.service[LogConf].flatMap(confLayer => layer(confLayer.get))
  lazy val default: ULayer[Unit]        = layer(LogConf())

  /**
   * MDC keys that allowed to be received from non-zio Slf4j pipeline.
   */
  private val defaultAcceptedSlf4jMdc = Set(Slf4jBridge.loggerNameAnno, Slf4jBridge.threadNameAnno)
  private val excludeAnnoKeys         = Set(Slf4jBridge.loggerNameAnno, Slf4jBridge.threadNameAnno)

  /**
   * Generate Potamoi ZIO logger layer.
   *
   * @param level     logging level.
   * @param style     Log line style.
   * @param inOneLine Logging on the same line.
   * @param colored   Whether to colorize log line.
   * @param appendLf  Appended log format content.
   */
  def layer(
      level: LogsLevel = LogsLevel.Info,
      style: LogsStyle = LogsStyle.Plain,
      colored: Boolean = true,
      inOneLine: Boolean = false,
      allowedMdc: Set[String] = Set.empty,
      appendLf: Option[LogFormat] = None): ULayer[Unit] = {
    val logFormat = if (colored) stLogFormatColored(appendLf, inOneLine) else stLogFormat(appendLf, inOneLine)
    val logLevel  = level.zioLevel
    val logLayer = style match {
      case LogsStyle.Plain => console(logFormat, logLevel)
      case LogsStyle.Json  => consoleJson(logFormat, logLevel)
    }
    zio.Runtime.removeDefaultLoggers >>> logLayer >+> Slf4jBridge.initialize(logLevel, (defaultAcceptedSlf4jMdc ++ allowedMdc).toVector)
  }

  /**
   * Generate Potamoi ZIO logger layer from [[LogConf]].
   */
  def layer(logConf: LogConf): ULayer[Unit] = layer(logConf.level, logConf.style, logConf.colored, logConf.inOneLine)

  /**
   * Standard log format for Potamoi.
   */
  private[potamoi] def stLogFormat(appendLogFormat: Option[LogFormat], inOneLine: Boolean): LogFormat = {
    label("ts", timestamp(DateTimeFormatter.ISO_LOCAL_DATE_TIME).fixed(26)) |-|
    label("level", level) |-|
    label("fiber", fiberId) |-|
    sourceLoc |-|
    logAnnoIfNonEmpty +
    appendLogFormat.map(_ + space).getOrElse(empty) +
    prettyMessage(inOneLine) +
    (if (inOneLine) space else empty) + label("cause", cause).filter(LogFilter.causeNonEmpty)
  }

  /**
   * Colored standard log format for Potamoi.
   */
  private[potamoi] def stLogFormatColored(appendLogFormat: Option[LogFormat], inOneLine: Boolean): LogFormat = {
    label("ts", timestamp(DateTimeFormatter.ISO_LOCAL_DATE_TIME).fixed(26)).color(LogColor.BLUE) |-|
    label("level", level).highlight |-|
    label("fiber", fiberId).color(LogColor.WHITE) |-|
    sourceLoc.color(LogColor.WHITE) |-|
    logAnnoIfNonEmpty.color(LogColor.WHITE) +
    appendLogFormat.map(_ + space).getOrElse(empty) +
    prettyMessage(inOneLine).highlight +
    (if (inOneLine) space else empty) + label("cause", cause).filter(LogFilter.causeNonEmpty)
  }

  private val fixedWidthSpaceStr = (0 until 30).map(_ => " ").mkString("")

  /**
   * Messages content logging formatter.
   */
  private def prettyMessage(inOneLine: Boolean): LogFormat = {
    def prettyLine(inOneLine: Boolean): LogFormat =
      LogFormat.make { (builder, _, _, _, line, _, _, _, _) =>
        Option(line()) match
          case None => builder
          case Some(lines) =>
            if (inOneLine) builder.appendText(lines.split('\n').map(_.trim).mkString(" "))
            else builder.appendText(lines.split('\n').mkString("\n" + fixedWidthSpaceStr))
      }
    (if (inOneLine) empty else newLine + text(fixedWidthSpaceStr)) + label("msg", quoted(prettyLine(inOneLine))).highlight
  }

  private[potamoi] val empty: LogFormat = {
    LogFormat.make { (builder, _, _, _, _, _, _, _, _) => builder }
  }

  /**
   * Source code location or Slf4j logger logging formatter.
   */
  private[potamoi] def sourceLoc: LogFormat = LogFormat.make { (builder, trace, _, _, _, _, _, _, annotations) =>
    (annotations.get("@loggerName"), trace) match
      // from slf4j
      case (Some(loggerName), _) =>
        annotations.get("@threadName") match
          case None => ()
          case Some(threadName) =>
            builder.appendKeyValue("thread", threadName)
            builder.appendText(" ")
        builder.appendKeyValue("loc", loggerName)
      // from zio system
      case (None, Trace(location, file, line)) =>
        builder.appendKeyValue("loc", location)
        builder.appendText(" ")
        builder.appendKeyValue("file", file)
        builder.appendText(" ")
        builder.appendKeyValue("line", line.toString)
      case _ => ()
  }

  /**
   * ZIO annotation logging formatter.
   */
  private[potamoi] def logAnnoIfNonEmpty: LogFormat = LogFormat.make { (builder, _, _, _, _, _, _, _, annotations) =>
    annotations.foreach { case (key, value) =>
      if (!excludeAnnoKeys.contains(key)) {
        builder.appendKeyValue(key, value)
        builder.appendText(" ")
      }
    }
  }

}
