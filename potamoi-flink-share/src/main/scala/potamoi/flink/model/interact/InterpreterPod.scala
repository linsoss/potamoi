package potamoi.flink.model.interact

import potamoi.curTs
import potamoi.flink.FlinkMajorVer
import zio.json.JsonCodec
import potamoi.flink.FlinkMajorVers.given_JsonCodec_FlinkMajorVer

case class InterpreterPod(flinkVer: FlinkMajorVer, host: String, port: Int, ts: Long = curTs) derives JsonCodec
