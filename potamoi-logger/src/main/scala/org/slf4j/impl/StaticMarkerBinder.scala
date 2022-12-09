package org.slf4j.impl

import org.slf4j.IMarkerFactory
import org.slf4j.helpers.BasicMarkerFactory
import org.slf4j.spi.MarkerFactoryBinder

class StaticMarkerBinder extends MarkerFactoryBinder {
  private val markerFactory       = BasicMarkerFactory()
  private val markerFactoryClzStr = classOf[BasicMarkerFactory].getName

  override def getMarkerFactory: IMarkerFactory = markerFactory
  override def getMarkerFactoryClassStr: String = markerFactoryClzStr
}

object StaticMarkerBinder {
  val SINGLETON: StaticMarkerBinder    = StaticMarkerBinder()
  def getSingleton: StaticMarkerBinder = StaticMarkerBinder.SINGLETON
}
