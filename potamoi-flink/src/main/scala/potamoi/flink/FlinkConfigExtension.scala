package potamoi.flink

import org.apache.flink.configuration.Configuration
import potamoi.flink.model.FlinkRawConfig

import scala.collection.immutable.{Iterable, TreeMap}
import scala.jdk.CollectionConverters.*

/**
 * Extension for Flink [[Configuration]]
 */
object FlinkConfigExtension:

  given Conversion[Configuration, ConfigurationPF] = ConfigurationPF(_)
  given Conversion[ConfigurationPF, Configuration] = _.value

  /**
   * Partition function for [[Configuration]].
   */
  class ConfigurationPF(conf: Configuration) {

    /**
     * Append key value to internal configuration.
     */
    def append(key: String, value: Any): ConfigurationPF = {
      val rawValue = value match {
        case v: Map[_, _]   => v.map(kv => s"${kv._1}=${kv._2}").mkString(";")
        case v: Iterable[_] => v.map(_.toString).mkString(";")
        case v: Array[_]    => v.map(_.toString).mkString(";")
        case v              => v.toString
      }
      conf.setString(key, rawValue)
      this
    }

    /**
     * Append [[FlinkRawConf]] to internal configuration.
     */
    def append(rawConf: FlinkRawConfig): ConfigurationPF =
      rawConf.mapping.foldLeft(this) { case (conf, (key, value)) =>
        conf.append(key, value)
      }

    def append(rawConf: Option[FlinkRawConfig]): ConfigurationPF = rawConf.map(append).getOrElse(this)

    /**
     * Tapping internal configuration.
     */
    def tap(f: Configuration => Any): Configuration = {
      f(this.conf)
      this
    }

    /**
     * Handling internal configuration via monand.
     */
    def pipe(f: ConfigurationPF => ConfigurationPF): ConfigurationPF                       = f(this)
    def pipeWhen(cond: => Boolean)(f: ConfigurationPF => ConfigurationPF): ConfigurationPF = if (cond) f(this) else this

    /**
     * Merge key value in Map to internal configuration.
     */
    def merge(anotherConf: Map[String, String]): ConfigurationPF = {
      anotherConf.foreach { case (k, v) => conf.setString(k, v) }
      this
    }

    /**
     * Get internal configuration.
     */
    def value: Configuration = conf

    /**
     * Convert to internal configuration to Map with confidential value handling settings.
     */
    def toMap(protect: Boolean = false): Map[String, String] = {
      val map = TreeMap(conf.toMap.asScala.toArray: _*)
      if (protect) map.map { case (k, v) => if (protectedFlinkConfigKeys.contains(k)) k -> "***" else k -> v }
      else map
    }
  }

  /**
   * The keys in the flink configuration that may need to be protected.
   */
  val protectedFlinkConfigKeys = Vector(
    "hive.s3.aws-secret-key",
    "fs.s3a.secret.key",
    "s3.secret-key",
    InjectedExecModeKey,
    InjectedDeploySourceConf._1
  )

  lazy val InjectedDeploySourceConf = "deploy.source" -> "potamoi"
  lazy val InjectedExecModeKey      = "exec.mode"
