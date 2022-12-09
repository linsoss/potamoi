package potamoi.common

import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}

/**
 * Scala major version.
 */
enum ScalaVer(val major: String):
  case scala211 extends ScalaVer("2.11")
  case scala212 extends ScalaVer("2.12")
  case scala213 extends ScalaVer("2.13")
  case scala3   extends ScalaVer("3")
  case unknown  extends ScalaVer("unknown")

object ScalaVer:
  def ofMajor(major: String): ScalaVer = ScalaVer.values.find(_.major == major).getOrElse(ScalaVer.unknown)
  given JsonCodec[ScalaVer] = JsonCodec(
    JsonEncoder[String].contramap(_.toString),
    JsonDecoder[String].map(ScalaVer.valueOf)
  )
