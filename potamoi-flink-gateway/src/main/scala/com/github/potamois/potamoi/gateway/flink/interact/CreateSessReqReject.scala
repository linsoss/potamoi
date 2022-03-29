package com.github.potamois.potamoi.gateway.flink.interact

import com.github.potamois.potamoi.gateway.flink.FlinkVersion.{FlinkVerSign, FlinkVerSignRange}


/**
 * Rejection reason for [[FsiSessManager.CreateSession]] command.
 *
 * @author Al-assad
 */
sealed trait CreateSessReqReject {
  def reason: String
}


case class UnsupportedFlinkVersion(reason: String = "") extends CreateSessReqReject

object UnsupportedFlinkVersion {
  def apply(flinkVer: FlinkVerSign): UnsupportedFlinkVersion = UnsupportedFlinkVersion(
    s"Flink version $flinkVer is not potamoi support list: [${FlinkVerSignRange.mkString(",")}]")
}


case class NoActiveFlinkGatewayService(reason: String = "") extends CreateSessReqReject

object NoActiveFlinkGatewayService {
  def apply(flinkVer: FlinkVerSign): NoActiveFlinkGatewayService = NoActiveFlinkGatewayService(
    s"There are no any active flink gateway service in current cluster that match the corresponding flink version $flinkVer" )
}




