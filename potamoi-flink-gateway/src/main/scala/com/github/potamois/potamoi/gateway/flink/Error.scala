package com.github.potamois.potamoi.gateway.flink

import com.github.potamois.potamoi.commons.{CborSerializable, curTs}

/**
 * Error information.
 *
 * @param summary summary message
 * @param stack   exception stack
 * @author Al-assad
 */
case class Error(summary: String, stack: Throwable) extends CborSerializable {
  def toTsError(ts: Long = curTs): TsError = TsError(summary, stack, ts)
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
}
