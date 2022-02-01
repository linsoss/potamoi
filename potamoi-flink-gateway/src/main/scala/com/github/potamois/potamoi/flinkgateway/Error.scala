package com.github.potamois.potamoi.flinkgateway


case class Error(summary: String, errorStack: String)

object Error {
  def apply(summary: String): Error = Error(summary, "")
  def apply(summary: String, cause: Throwable): Error = Error(summary, cause.getMessage)
}
