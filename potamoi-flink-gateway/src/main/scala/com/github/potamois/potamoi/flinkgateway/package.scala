package com.github.potamois.potamoi

/**
 * @author Al-assad
 */
package object flinkgateway {

  case class SafeResult[T](pass: Boolean, error: Option[String], payload: Option[T])

}
