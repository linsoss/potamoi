package potamoi.flink.model.interact

import potamoi.cluster.Address
import potamoi.flink.FlinkMajorVer
import zio.json.JsonCodec
import potamoi.flink.FlinkMajorVers.given_JsonCodec_FlinkMajorVer

/**
 * Flink remote interpreter node info
 */
case class InterpreterNode(flinkVer: FlinkMajorVer, address: Address, receptionistActorPath: String) derives JsonCodec
