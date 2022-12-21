package potamoi.common

import zio.{Cause, Chunk, StackTrace, Trace, UIO, ZIO}

import java.io.{PrintWriter, StringWriter}
import scala.annotation.tailrec
import scala.util.control.NoStackTrace

/**
 * Throwable parent of potamoi effect that always do not fill
 * stacktrace for efficiency reasons.
 */
abstract class Err(message: String, cause: Throwable) extends Throwable(message, cause, true, false):
  def this(message: String) = this(message, null)
  def this() = this("", null)

object ErrorExtension:

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
   * Recursive rendering the [[Throwable]] cause of the [[Cause]] into a new TraceStack.
   * Use cases:
   * {{{
   *    ZIO.logErrorCause(cause.headMessage, cause.recurse)
   *    ZIO.logErrorCause(cause.recurse)
   * }}}
   */
  extension (cause: Cause[Throwable]) {
    inline def recurse: Cause[Throwable] = recurseCause(cause)
    inline def headMessage: String       = cause.failures.headOption.map(_.getMessage).getOrElse("")
  }

  /**
   * Recursive rendering all of sub [[Throwable]] cause into current [[Cause]].
   */
  def recurseCause(cause: Cause[Throwable]): Cause[Throwable] = {
    @tailrec def recurseRender(cause: Cause[Throwable], preStackChunk: Chunk[Trace], curError: Option[Throwable]): Cause[Throwable] = curError match {
      case None => cause
      case Some(curErr) =>
        val curStackChunk = Chunk.fromArray(curErr.getStackTrace).map(Trace.fromJava).reverse
        val effectedChunk = curStackChunk
          .zipAll(preStackChunk)
          .dropWhile {
            case (Some(curTrace), Some(preTrace)) => Trace.equalIgnoreLocation(curTrace, preTrace)
            case _                                => false
          }
          .filter(_._1.isDefined)
          .map(_._1.get)
          .reverse
        val newCause = cause ++ Cause.fail(curErr, StackTrace(cause.trace.fiberId, effectedChunk))
        recurseRender(newCause, curStackChunk, Option(curErr.getCause))
    }

    cause.failures.headOption match {
      case None => cause
      case Some(err) =>
        recurseRender(cause, Chunk.fromArray(err.getStackTrace).map(Trace.fromJava).reverse, Option(err.getCause))
    }
  }

  private def traceEqualsLocation(left: Trace, right: Trace): Boolean = (left, right) match
    case (Trace(leftLoc, _, _), Trace(rightLoc, _, _)) => leftLoc == rightLoc
