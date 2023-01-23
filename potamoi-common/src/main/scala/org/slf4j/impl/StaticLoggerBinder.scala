package org.slf4j.impl

import org.slf4j.ILoggerFactory
import org.slf4j.spi.LoggerFactoryBinder

class StaticLoggerBinder extends LoggerFactoryBinder {
  private val loggerFactory    = ZIOLoggerFactory()
  private val loggerFactoryStr = ZIOLoggerFactory.getClass.getName

  override def getLoggerFactory: ILoggerFactory = loggerFactory
  override def getLoggerFactoryClassStr: String = loggerFactoryStr
}

object StaticLoggerBinder {
  val SINGLETON: StaticLoggerBinder    = new StaticLoggerBinder
  val REQUESTED_API_VERSION: String    = "1.6.99"
  def getSingleton: StaticLoggerBinder = SINGLETON
}
