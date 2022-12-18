package potamoi.common

import zio.{Cause, Chunk, StackTrace, Trace, UIO, ZIO}

import scala.util.control.NoStackTrace
import java.io.StringWriter
import java.io.PrintWriter
import scala.annotation.tailrec

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
    @tailrec def recurseRender(cause: Cause[Throwable], cur: Option[Throwable]): Cause[Throwable] = cur match {
      case None => cause
      case Some(err) =>
        val headTrace = cause.trace.stackTrace.head
        val traceChunk = {
          val stackTraceArr = err.getStackTrace
          val slice = Chunk
            .fromArray(stackTraceArr)
            .map(Trace.fromJava)
            .takeWhile(!traceEqualsLocation(_, headTrace))
          if slice.size == stackTraceArr.length then slice else slice :+ Trace.fromJava(stackTraceArr(slice.size))
        }
        val newCause = cause ++ Cause.fail(err, StackTrace(cause.trace.fiberId, traceChunk))
        recurseRender(newCause, Option(err.getCause))
    }
    recurseRender(cause, cause.failures.headOption.flatMap(e => Option(e.getCause)))
  }

  private def traceEqualsLocation(left: Trace, right: Trace): Boolean = (left, right) match
    case (Trace(leftLoc, _, _), Trace(rightLoc, _, _)) => leftLoc == rightLoc
