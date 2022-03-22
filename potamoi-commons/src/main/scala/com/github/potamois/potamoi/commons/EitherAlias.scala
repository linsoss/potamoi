package com.github.potamois.potamoi.commons

/**
 * Alias for Scala [[Either]].
 *
 * @author Al-assad
 */
object EitherAlias {

  /**
   * Alias for [[Left]]
   */
  type fail[+A, +B] = scala.util.Left[A, B]
  val fail: Left.type = scala.util.Left

  /**
   * Alias for [[Right]]
   */
  type success[+A, +B] = scala.util.Right[A, B]
  val success: Right.type = scala.util.Right

}

