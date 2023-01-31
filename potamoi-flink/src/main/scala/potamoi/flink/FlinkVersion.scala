package potamoi.flink

import potamoi.codecs
import potamoi.common.ScalaVersion
import potamoi.common.ScalaVersion.Scala212
import potamoi.common.ScalaVersions.given_JsonCodec_ScalaVersion
import potamoi.flink.FlinkVersion
import potamoi.flink.FlinkVersion.extractMajorVer
import potamoi.syntax.contra
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Flink version.
 */
case class FlinkVersion(ver: String, scalaVer: ScalaVersion = Scala212) derives JsonCodec:
  def majorVer: String = extractMajorVer(ver)
  def fullVer: String  = s"${ver}-scala_${scalaVer}"

object FlinkVersion:
  given Conversion[(String, ScalaVersion), FlinkVersion] = params => FlinkVersion(params._1, params._2)

  def extractMajorVer(flinkVer: String): String = flinkVer.split('.').contra { part =>
    if (part.length < 2) flinkVer
    else part(0) + "." + part(1)
  }

/**
 * Flink Major version.
 */
enum FlinkMajorVer(val value: String):
  case V116 extends FlinkMajorVer("1.16")
  case V115 extends FlinkMajorVer("1.15")

object FlinkMajorVers:
  given JsonCodec[FlinkMajorVer] = codecs.stringBasedJsonCodec(e => e.value, s => FlinkMajorVer.values.find(_.value == s))
