package potamoi.flink

import org.apache.flink.configuration.Configuration
import potamoi.flink.model.deploy.FlinkProps
import potamoi.flink.operator.resolver.ClusterSpecResolver.ProtectedFlinkConfigKeys

import scala.collection.immutable.TreeMap
import scala.jdk.CollectionConverters.*

object FlinkConfigurationTool:

  type ConfigValue = BasicValue | Option[BasicValue] | Seq[BasicValue] | Map[String, BasicValue]
  type BasicValue  = String | Boolean | Int | Long | Double | Float

  extension (config: Configuration) {

    private def setBasicValue(key: String, value: BasicValue) = value match {
      case v: String  => config.setString(key, v)
      case v: Boolean => config.setBoolean(key, v)
      case v: Int     => config.setInteger(key, v)
      case v: Long    => config.setLong(key, v)
      case v: Double  => config.setDouble(key, v)
      case v: Float   => config.setFloat(key, v)
    }

    private def valueToString(value: BasicValue) = value match {
      case v: String  => v
      case v: Boolean => v.toString
      case v: Int     => v.toString
      case v: Long    => v.toString
      case v: Double  => v.toString
      case v: Float   => v.toString
    }

    /**
     * Set the key-value of configuration in a type-safe way.
     */
    def safeSet(key: String, value: ConfigValue, ignoreEmpty: Boolean = true): Configuration = {
      value match
        case v: BasicValue                => setBasicValue(key, v)
        case opt: Option[BasicValue]      =>
          opt match
            case None     => if !ignoreEmpty then config.setString(key, "")
            case Some(vv) => setBasicValue(key, vv)
        case seq: Seq[BasicValue]         =>
          seq match
            case seq if seq.isEmpty => if !ignoreEmpty then config.setString(key, "")
            case seq                => config.setString(key, seq.map(valueToString).mkString(";"))
        case map: Map[String, BasicValue] =>
          map match
            case map if map.isEmpty => if !ignoreEmpty then config.setString(key, "")
            case map                => config.setString(key, map.map { case (k, v) => s"$k=${valueToString(v)}" }.mkString(";"))
      config
    }

    /**
     * Merge the mapping configs of [[FlinkProps]] into Configuration.
     */
    def mergePropOpt(flinkPropOpt: Option[FlinkProps]): Configuration = flinkPropOpt.map(mergeProp).getOrElse(config)

    def mergeProp(flinkProp: FlinkProps): Configuration = flinkProp.mapping.foldLeft(config) { case (conf, (key, value)) =>
      conf.safeSet(key, value)
    }

    def mergeMap(configMap: Map[String, String]): Configuration = {
      configMap.foreach { case (key, value) => config.setString(key, value) }
      config
    }

    /**
     * Convert to internal configuration to Map with confidential value handling settings.
     */
    def dumpToMap(enableProtect: Boolean = false): Map[String, String] = {
      val map = TreeMap(config.toMap.asScala.toArray: _*)
      if (enableProtect) map.map { case (k, v) => if (ProtectedFlinkConfigKeys.contains(k)) k -> "***" else k -> v }
      else map
    }
  }
