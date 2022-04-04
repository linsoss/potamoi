package com.github.potamois.potamoi.gateway.flink

import com.github.potamois.potamoi.akka.serialize.CborSerializable
import com.github.potamois.potamoi.commons.curTs

/**
 * Error information.
 *
 * @param summary summary message
 * @param stack   exception stack
 * @author Al-assad
 */
case class Error(summary: String, stack: Throwable) extends CborSerializable {
  def toTsError(ts: Long = curTs): TsError = TsError(summary, stack, ts)
  def printStackTrace(): Unit = stack.printStackTrace()
}

/**
 * Error information with timestamp marked.
 *
 * @param summary summary message
 * @param stack   exception stack
 * @param ts      error timestamp marked
 */
case class TsError(summary: String, stack: Throwable, ts: Long) extends CborSerializable {
  def toError: Error = Error(summary, stack)
  def printStackTrace(): Unit = stack.printStackTrace()
}
