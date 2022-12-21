package potamoi.common

import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}

import scala.concurrent.duration.{durationToPair, Duration}

object Codec:

  given scalaDurationJsonCodec: JsonCodec[Duration] = JsonCodec(
    encoder = JsonEncoder[(Long, String)].contramap(d => d._1 -> d._2.toString),
    decoder = JsonDecoder[(Long, String)].map(t => Duration(t._1, t._2))
  )
