package org.slf4j.impl

import org.slf4j.{ILoggerFactory, Logger}
import zio.{LogLevel, Unsafe, ZIO}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters.*

//noinspection ConvertNullInitializerToUnderscore
class ZIOLoggerFactory extends ILoggerFactory {

  private val loggers                    = new ConcurrentHashMap[String, Logger]().asScala
  private var runtime: zio.Runtime[Any]  = null
  private var mdcFilters: Vector[String] = Vector.empty
  private var level: LogLevel            = LogLevel.Info

  private def attachRuntime(runtime: zio.Runtime[Any]): Unit = this.runtime = runtime
  private def setFilterMdc(mdcFilters: Vector[String])       = this.mdcFilters = mdcFilters
  private def setLogLevel(level: LogLevel)                   = this.level = level
  def getFilterMdc                                           = mdcFilters
  def getLogLevel                                            = level

  private[slf4j] def run(f: ZIO[Any, Nothing, Any]): Unit = {
    if (runtime != null) Unsafe.unsafe { implicit u => runtime.unsafe.run(f) }
  }

  override def getLogger(name: String): Logger = {
    loggers.getOrElseUpdate(name, new ZIOLogger(name, this))
  }

}

object ZIOLoggerFactory {
  def initialize(runtime: zio.Runtime[Any], level: LogLevel, mdcKeys: Vector[String]): Unit = {
    val factory = StaticLoggerBinder.SINGLETON.getLoggerFactory.asInstanceOf[ZIOLoggerFactory]
    factory.attachRuntime(runtime)
    factory.setLogLevel(level)
    factory.setFilterMdc(mdcKeys)
  }
}
