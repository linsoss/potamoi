package com.github.potamois.potamoi

/**
 * @author Al-assad
 */
// noinspection SpellCheckingInspection
package object flinkgateway {

  case class SafeResult[T](pass: Boolean, error: Option[Error], payload: Option[T])

  case class Error(summary: String, errorStack: String)

  object SafeResult {
    def pass[T](payloadValue: T): SafeResult[T] = SafeResult(pass = true, None, Some(payloadValue))

    def fail[T](error: Error): SafeResult[T] = SafeResult(pass = false, Some(error), None)

    def fail[T](errorSummary: String): SafeResult[T] = SafeResult(pass = false, Some(Error(errorSummary, "")), None)
  }

}
