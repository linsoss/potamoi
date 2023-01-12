package potamoi.common

import zio.{Cause, UIO, ZIO}

import java.io.{PrintWriter, StringWriter}
import potamoi.syntax.toPrettyStr

/**
 * Error AST root of potamoi effect.
 */
trait PotaErr

object PotaErr:

  /**
   * Dump stack trace of [[Throwable]] to formatted string.
   */
  extension (err: Throwable)
    def stackTraceString: String = {
      val sw = StringWriter()
      val pw = PrintWriter(sw)
      err.printStackTrace(pw)
      sw.toString
    }

  /**
   * Pretty logging of PotaErr type Cause.
   */
  def logErrorCausePretty(cause: Cause[PotaErr]): UIO[Unit] =
    ZIO.logErrorCause(cause.failureOption.map(_.toPrettyStr).getOrElse(""), cause)
