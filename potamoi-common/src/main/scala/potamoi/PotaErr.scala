package potamoi

import potamoi.syntax.toPrettyString
import zio.{Cause, UIO, ZIO}

import java.io.{PrintWriter, StringWriter}

/**
 * Error AST root of potamoi effect which is a stackless Throwable.
 */
trait PotaErr extends Throwable:
  override def fillInStackTrace: Throwable = this
  override def getMessage: String          = toPrettyString(this)

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
    ZIO.logErrorCause(cause.failureOption.map(toPrettyString).getOrElse(""), cause)

  def logErrorCausePretty(message: String, cause: Cause[PotaErr]): UIO[Unit] =
    ZIO.logErrorCause(s"$message, cause: ${cause.failureOption.map(toPrettyString).getOrElse("")}", cause)

end PotaErr
