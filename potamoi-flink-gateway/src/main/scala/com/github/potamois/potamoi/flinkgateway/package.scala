package com.github.potamois.potamoi

/**
 * @author Al-assad
 */
// noinspection SpellCheckingInspection
package object flinkgateway {

  case class SafeResult[T](pass: Boolean, error: Option[Error], payload: Option[T])

  case class Error(summary: String, errorStack: String)

  object SafeResult {
    def pass[T](payload: Option[T]): SafeResult[T] = SafeResult(pass = true, None, payload)

    def fail[T](error: Error): SafeResult[T] = SafeResult(pass = false, Some(error), None)

    def fail[T](errorSummary: String): SafeResult[T] = SafeResult(pass = false, Some(Error(errorSummary, "")), None)
  }

}
