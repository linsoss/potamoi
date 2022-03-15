package com.github.potamois.potamoi.gateway.flink.interact

/**
 * Warning message for refusing to execute sqls.
 *
 * @author Al-assad
 */
sealed trait ExecReqReject {
  def reason: String
}

/**
 * The executor is busy and there is currently a sql statement that is
 * still being executed.
 *
 * @param reason  rejection reason
 * @param startTs start timestamp of the sql statements execution plan in process
 * @author Al-assad
 */
case class BusyInProcess(reason: String, startTs: Long) extends ExecReqReject



