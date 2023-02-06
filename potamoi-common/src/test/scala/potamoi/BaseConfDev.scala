package potamoi

import zio.{ULayer, ZLayer}

object BaseConfDev:

  given Conversion[BaseConf.type, BaseConfDev.type] = _ => BaseConfDev

  val testValue: BaseConf = BaseConf(
    svcDns = "10.144.108.28",
    dataDir = "var/potamoi"
  )

  val test: ULayer[BaseConf] = ZLayer.succeed(testValue)
