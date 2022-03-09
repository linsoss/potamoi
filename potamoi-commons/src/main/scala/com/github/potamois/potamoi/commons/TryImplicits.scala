package com.github.potamois.potamoi.commons

import scala.util.Try

/**
 * Enhancement function for [[Try]].
 *
 * @author Al-assad
 */
object TryImplicits extends App {

    implicit class Wrapper[T](val t: Try[T]) {
      /**
       * Simplification of "fold(fa, identity)"
       */
      def foldIdentity(func: Throwable => T): T = t.fold(func, identity)
    }

}

