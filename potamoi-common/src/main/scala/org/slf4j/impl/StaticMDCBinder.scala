package org.slf4j.impl

import org.slf4j.helpers.BasicMDCAdapter
import org.slf4j.spi.MDCAdapter

class StaticMDCBinder {
  private val mdcAdapter       = BasicMDCAdapter()
  private val mdcAdapterClzStr = classOf[BasicMDCAdapter].getName

  def getMDCA: MDCAdapter           = mdcAdapter
  def getMDCAdapterClassStr: String = mdcAdapterClzStr
}

object StaticMDCBinder {
  val SINGLETON: StaticMDCBinder    = StaticMDCBinder()
  def getSingleton: StaticMDCBinder = SINGLETON
}
