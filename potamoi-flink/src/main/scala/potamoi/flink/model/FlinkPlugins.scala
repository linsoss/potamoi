package potamoi.flink.model

import potamoi.flink.model.FlinkVersion
import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}

import scala.util.Try

/**
 * Flink build in plugins.
 */
enum FlinkPlugin(val name: String, jarNameFunc: (String, FlinkVersion) => String) {

  case S3Hadoop      extends FlinkPlugin("flink-s3-fs-hadoop", scalaFree)
  case S3HadoopGS    extends FlinkPlugin("flink-gs-fs-hadoop", scalaFree)
  case S3HadoopOSS   extends FlinkPlugin("flink-oss-fs-hadoop", scalaFree)
  case S3HadoopAzure extends FlinkPlugin("flink-azure-fs-hadoop", scalaFree)
  case S3Presto      extends FlinkPlugin("flink-s3-fs-presto", scalaFree)
  case Cep           extends FlinkPlugin("flink-cep", scalaBind)
  case Gelly         extends FlinkPlugin("flink-gelly", scalaBind)
  case PyFlink       extends FlinkPlugin("flink-python", scalaBind)

  /**
   * Convert to flink jar name with flink version.
   */
  def jarName: FlinkVersion => String = jarNameFunc(name, _)
}

object FlinkPlugins {

  import FlinkPlugin.*

  given JsonCodec[FlinkPlugin] = JsonCodec(
    JsonEncoder[String].contramap(_.toString),
    JsonDecoder[String].mapOrFail(s => values.find(_.toString == s).toRight(s"Unknown FlinkPlugin: $s"))
  )

  /**
   * All built-in plugins.
   */
  val Plugins = values.toSet

  /**
   * S3 built-in plugins.
   */
  lazy val S3Plugins  = S3aPlugins ++ Set(S3Presto)
  lazy val S3aPlugins = Set(S3Hadoop, S3HadoopGS, S3HadoopOSS, S3HadoopAzure)

  /**
   * HDFS built-in plugins
   */
  lazy val HadoopPlugins       = Set(S3Hadoop, S3HadoopGS, S3HadoopOSS, S3HadoopAzure)
  lazy val DefaultHadoopPlugin = S3Hadoop

}

private lazy val scalaFree = (name: String, v: FlinkVersion) => s"${name}-${v.ver}.jar"
private lazy val scalaBind = (name: String, v: FlinkVersion) => s"${name}_${v.scalaVer}-${v.ver}.jar"
