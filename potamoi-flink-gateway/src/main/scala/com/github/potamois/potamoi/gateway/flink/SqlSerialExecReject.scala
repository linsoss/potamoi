package com.github.potamois.potamoi.gateway.flink

/**
 * Warning message for refusing to execute sqls.
 *
 * @author Al-assad
 */
sealed trait ExecReject {
  def reason: String
}

/**
 * The executor is busy and there is currently a sql statement that is
 * still being executed.
 *
 * @param reason    rejection reason
 * @param statement sql statement that is currently being executed
 * @param startTs   start time of sql statement in process
 * @author Al-assad
 */
case class BusyInProcess(reason: String, statement: String, startTs: Long) extends ExecReject {
  def logMessage: String = s"$reason [statement]=$statement [startTs]=$startTs"
}



