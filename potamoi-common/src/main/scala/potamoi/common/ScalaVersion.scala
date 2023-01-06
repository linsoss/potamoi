package potamoi.common

import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}
import potamoi.codecs

/**
 * Scala major version.
 */
enum ScalaVersion(val value: String):
  case Scala211 extends ScalaVersion("2.11")
  case Scala212 extends ScalaVersion("2.12")
  case Scala213 extends ScalaVersion("2.13")
  case Scala3   extends ScalaVersion("3")
  case Unknown  extends ScalaVersion("unknown")

object ScalaVersions:
  given JsonCodec[ScalaVersion]            = codecs.simpleEnumJsonCodec(ScalaVersion.values)
  def ofMajor(major: String): ScalaVersion = ScalaVersion.values.find(_.value == major).getOrElse(ScalaVersion.Unknown)
