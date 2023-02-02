package potamoi.flink.model.interact

import potamoi.{curTs, KryoSerializable}
import potamoi.flink.{FlinkMajorVer, FlinkVersion}
import potamoi.flink.FlinkMajorVers.given_JsonCodec_FlinkMajorVer
import zio.json.JsonCodec

case class InteractSession(
    sessionId: String,
    flinkVer: FlinkMajorVer,
    createdAt: Long = curTs)
    extends KryoSerializable
    derives JsonCodec
