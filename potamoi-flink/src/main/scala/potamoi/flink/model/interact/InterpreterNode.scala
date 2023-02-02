package potamoi.flink.model.interact

import potamoi.flink.FlinkMajorVer
import zio.json.JsonCodec
import potamoi.flink.FlinkMajorVers.given_JsonCodec_FlinkMajorVer

/**
 * Flink remote interpreter node info
 */
case class InterpreterNode(flinkVer: FlinkMajorVer, host: Option[String], port: Option[Int], actorPath: String) derives JsonCodec
