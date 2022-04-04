package com.github.potamois.potamoi.gateway.flink.interact

import com.github.potamois.potamoi.akka.serialize.CborSerializable


/**
 * Flink sql serial operation execution result trait
 *
 * @author Al-assad
 */
trait OperationDone extends CborSerializable

/**
 * flink sql immediate operation execution result like "create ...","explain ..."
 */
case class ImmediateOpDone(data: TableResultData) extends OperationDone

/**
 * submit flink modify operation(like "insert...") done
 */
case class SubmitModifyOpDone(jobId: String) extends OperationDone {
  def toFriendlyString: String = s"Submit modify statement to Flink cluster successfully, jobId = $jobId"
}

/**
 * submit flink query operation(like "select...") done
 */
case class SubmitQueryOpDone(jobId: String) extends OperationDone {
  def toFriendlyString: String = s"Submit query statement to Flink cluster successfully, jobId = $jobId"
}
