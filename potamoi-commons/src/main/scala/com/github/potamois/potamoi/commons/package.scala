package com.github.potamois.potamoi

import java.io.{PrintWriter, StringWriter}
import scala.collection.mutable
import scala.util.Try

/**
 * commons tools
 *
 * @author Al-assad
 */
package object commons {

  /**
   * Get current timestamp from system.
   */
  def curTs: Long = System.currentTimeMillis

  /**
   * Enhancement for [[String]]
   */
  implicit class RichString(str: String) {
    /**
     * Remove "\n" from string
     */
    def compact: String = str.split("\n").map(_.trim).filter(_.nonEmpty).mkString(" ")
  }

  /**
   * Enhancement for [[Try]]
   */
  implicit class RichTry[T](val t: Try[T]) {
    /**
     * Simplification of "fold(fa, identity)"
     */
    def foldIdentity(func: Throwable => T): T = t.fold(func, identity)
  }

  /**
   * Enhancement for [[Throwable]]
   */
  implicit class RichThrowable(e: Throwable) {
    /**
     * Get stack trace as string from Throwable
     */
    def getStackTraceAsString: String = {
      if (e == null) ""
      else {
        val sw = new StringWriter
        val pw = new PrintWriter(sw)
        e.printStackTrace(pw)
        sw.getBuffer.toString
      }
    }
  }

  /**
   * Enhancement for [[Map]]
   */
  implicit class RichMap[K, V](map: Map[K, V]) {
    /**
     * When the map does not contain a key, set the value, otherwise return the original map.
     */
    def ?+(kv: (K, V)): Map[K, V] = if (map.contains(kv._1)) map else map + kv
  }

  /**
   * Enhancement for [[mutable.Map]]
   */
  implicit class RichMutableMap[K, V](map: mutable.Map[K, V]) {
    /**
     * When the map does not contain a key, set the value, otherwise return the original map.
     */
    def ?+=(kv: (K, V)): mutable.Map[K, V] = if (map.contains(kv._1)) map else map += kv
  }


}
