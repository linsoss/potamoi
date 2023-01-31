package potamoi.flink

import org.apache.flink.configuration.Configuration

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

  }
