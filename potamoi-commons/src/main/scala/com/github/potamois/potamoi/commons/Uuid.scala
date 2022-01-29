package com.github.potamois.potamoi.commons

/**
 * UUID generator
 *
 * @author Al-assad
 */
object Uuid {

  /**
   * Generate a uuid of length 36
   */
  def genUUID: String = java.util.UUID.randomUUID().toString

  /**
   * Generate a uuid of length 32
   */
  def genShortUUID: String = genUUID.split("-").mkString


}
