package potamoi.common

import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}

import scala.concurrent.duration.{durationToPair, Duration}

object Codec:

  given scalaDurationJsonCodec: JsonCodec[Duration] =
    JsonCodec(
      encoder = JsonEncoder[(Long, String)].contramap(d => d._1 -> d._2.toString),
      decoder = JsonDecoder[(Long, String)].map(t => Duration(t._1, t._2))
    )

  inline def stringBasedJsonCodec[T](encode: T => String, decode: String => Option[T]): JsonCodec[T] =
    JsonCodec(
      encoder = JsonEncoder[String].contramap(encode),
      decoder = JsonDecoder[String].mapOrFail(s => decode(s).toRight(s"Name $s is invalid value"))
    )

  inline def simpleEnumJsonCodec[T](offer: Array[T]): JsonCodec[T] =
    stringBasedJsonCodec(_.toString, s => offer.find(_.toString == s))
