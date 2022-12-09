package org.slf4j.impl

import org.slf4j.MDC
import org.slf4j.helpers.{MarkerIgnoringBase, MessageFormatter}
import potamoi.logger.Slf4jBridge
import zio.{Cause, LogLevel, UIO, ZIO, ZIOAspect}

import scala.collection.JavaConverters.*

class ZIOLogger(name: String, factory: ZIOLoggerFactory) extends MarkerIgnoringBase {

  private val shouldLogTrace   = factory.getLogLevel >= LogLevel.Trace
  private val shouldLogDebug   = factory.getLogLevel >= LogLevel.Debug
  private val shouldLogInfo    = factory.getLogLevel >= LogLevel.Info
  private val shouldLogWarning = factory.getLogLevel >= LogLevel.Warning
  private val shouldLogError   = factory.getLogLevel >= LogLevel.Error

  private val mdcKeys                              = factory.getFilterMdc
  private val loggerNameAnnoKv                     = Slf4jBridge.loggerNameAnno -> name
  private def threadNameAnnoKv(threadName: String) = Slf4jBridge.threadNameAnno -> threadName

  private def run(f: (String, ZIO[Any, Nothing, Unit])): Unit = {
    factory.run(ZIO.logSpan(name) {
      injectZIOLogAnno(f._1, f._2)
    })
  }

  /**
   * Inject MDC, thread name, logger name into ZIO logging annotation context.
   */
  private def injectZIOLogAnno(threadName: String, f: UIO[Unit]) = {
    val mdcAnnoKvs = {
      val mdcMap = MDC.getCopyOfContextMap
      if (MDC.getCopyOfContextMap == null) Vector()
      else mdcMap.asScala.filter(kv => mdcKeys.contains(kv._1)).toVector
    }
    val annoKvs = Vector(loggerNameAnnoKv, threadNameAnnoKv(threadName)) ++ mdcAnnoKvs
    f @@ ZIOAspect.annotated(annoKvs: _*)
  }

  private def curThreadName: String = Thread.currentThread().getName

  override def isTraceEnabled: Boolean = shouldLogTrace

  override def isDebugEnabled: Boolean = shouldLogDebug

  override def isInfoEnabled: Boolean = shouldLogInfo

  override def isWarnEnabled: Boolean = shouldLogWarning

  override def isErrorEnabled: Boolean = shouldLogError

  override def trace(msg: String): Unit = run {
    curThreadName -> ZIO.logTrace(msg)
  }

  override def trace(format: String, arg: AnyRef): Unit = run {
    curThreadName -> ZIO.logTrace(MessageFormatter.format(format, arg).getMessage)
  }

  override def trace(format: String, arg1: AnyRef, arg2: AnyRef): Unit = run {
    curThreadName -> ZIO.logTrace(MessageFormatter.format(format, arg1, arg2).getMessage)
  }

  override def trace(format: String, arguments: AnyRef*): Unit = run {
    curThreadName -> ZIO.logTrace(MessageFormatter.arrayFormat(format, arguments.toArray).getMessage)
  }

  override def trace(msg: String, t: Throwable): Unit = run {
    curThreadName -> ZIO.logTraceCause(msg, Option(t).map(t => Cause.die(t)).getOrElse(Cause.empty))
  }

  override def debug(msg: String): Unit = run {
    curThreadName -> ZIO.logDebug(msg)
  }

  override def debug(format: String, arg: AnyRef): Unit = run {
    curThreadName -> ZIO.logDebug(MessageFormatter.format(format, arg).getMessage)
  }

  override def debug(format: String, arg1: AnyRef, arg2: AnyRef): Unit = run {
    curThreadName -> ZIO.logDebug(MessageFormatter.format(format, arg1, arg2).getMessage)
  }

  override def debug(msg: String, t: Throwable): Unit = run {
    curThreadName -> ZIO.logDebugCause(msg, Option(t).map(t => Cause.die(t)).getOrElse(Cause.empty))
  }

  override def debug(format: String, arguments: AnyRef*): Unit = run {
    curThreadName -> ZIO.logDebug(MessageFormatter.arrayFormat(format, arguments.toArray).getMessage)
  }

  override def info(msg: String): Unit = run {
    curThreadName -> ZIO.logInfo(msg)
  }

  override def info(format: String, arg: AnyRef): Unit = run {
    curThreadName -> ZIO.logInfo(MessageFormatter.format(format, arg).getMessage)
  }

  override def info(format: String, arg1: AnyRef, arg2: AnyRef): Unit = run {
    curThreadName -> ZIO.logInfo(MessageFormatter.format(format, arg1, arg2).getMessage)
  }

  override def info(format: String, arguments: AnyRef*): Unit = run {
    curThreadName -> ZIO.logInfo(MessageFormatter.arrayFormat(format, arguments.toArray).getMessage)
  }

  override def info(msg: String, t: Throwable): Unit = run {
    curThreadName -> ZIO.logInfoCause(msg, Option(t).map(t => Cause.die(t)).getOrElse(Cause.empty))
  }

  override def warn(msg: String): Unit = run {
    curThreadName -> ZIO.logWarning(msg)
  }

  override def warn(format: String, arg: AnyRef): Unit = run {
    curThreadName -> ZIO.logWarning(MessageFormatter.format(format, arg).getMessage)
  }

  override def warn(format: String, arg1: AnyRef, arg2: AnyRef): Unit = run {
    curThreadName -> ZIO.logWarning(MessageFormatter.format(format, arg1, arg2).getMessage)
  }

  override def warn(format: String, arguments: AnyRef*): Unit = run {
    curThreadName -> ZIO.logWarning(MessageFormatter.arrayFormat(format, arguments.toArray).getMessage)
  }

  override def warn(msg: String, t: Throwable): Unit = run {
    curThreadName -> ZIO.logWarningCause(msg, Option(t).map(t => Cause.die(t)).getOrElse(Cause.empty))
  }

  override def error(msg: String): Unit = run {
    curThreadName -> ZIO.logError(msg)
  }

  override def error(format: String, arg: AnyRef): Unit = run {
    curThreadName -> ZIO.logError(MessageFormatter.format(format, arg).getMessage)
  }

  override def error(format: String, arg1: AnyRef, arg2: AnyRef): Unit = run {
    curThreadName -> ZIO.logError(MessageFormatter.format(format, arg1, arg2).getMessage)
  }

  override def error(format: String, arguments: AnyRef*): Unit = run {
    curThreadName -> ZIO.logError(MessageFormatter.arrayFormat(format, arguments.toArray).getMessage)
  }

  override def error(msg: String, t: Throwable): Unit = run {
    curThreadName -> ZIO.logErrorCause(msg, Option(t).map(t => Cause.die(t)).getOrElse(Cause.empty))
  }

}
