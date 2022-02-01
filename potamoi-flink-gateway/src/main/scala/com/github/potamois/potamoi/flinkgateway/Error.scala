package com.github.potamois.potamoi.flinkgateway

/**
 * @param summary
 * @param errorStack
 * @author Al-assad
 */
case class Error(summary: String, errorStack: String)

object Error {
  def apply(summary: String): Error = Error(summary, "")
  def apply(summary: String, cause: Throwable): Error = Error(summary, cause.getMessage)
}
