package com.github.potamois.potamoi

import scala.util.Try

package object commons {

  /**
   * Get current timestamp from system.
   */
  def curTs: Long = System.currentTimeMillis

  /**
   * Enhancement for [[String]]
   */
  implicit class RichString(str: String) {
    // remove "\n" from string
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




}
