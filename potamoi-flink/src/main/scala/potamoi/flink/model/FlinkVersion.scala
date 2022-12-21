package potamoi.flink.model

import potamoi.common.ScalaVersion
import potamoi.common.ScalaVersion.Scala212
import potamoi.flink.model.FlinkVersion.extractMajorVer
import potamoi.syntax.contra
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Flink version.
 */
case class FlinkVersion(ver: String, scalaVer: ScalaVersion = Scala212):
  def majorVer: String = extractMajorVer(ver)
  def fullVer: String  = s"${ver}-scala_${scalaVer}"

object FlinkVersion:
  import potamoi.common.ScalaVersions.given
  given JsonCodec[FlinkVersion] = DeriveJsonCodec.gen[FlinkVersion]

  def extractMajorVer(flinkVer: String): String = flinkVer.split('.').contra { part =>
    if (part.length < 2) flinkVer
    else part(0) + "." + part(1)
  }
