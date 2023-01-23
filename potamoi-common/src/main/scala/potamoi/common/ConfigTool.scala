package potamoi.common

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import zio.config.typesafe.TypesafeConfigSource
import zio.ZIO

import java.io.File

object ConfigTool:

  val ConfigPathKey = "POTAMOI_CONF_PATH"

  /**
   * Hocon formatted configuration source.
   *
   * Priority: java properties -> system env -> hocon file;
   * File path value priority: java properties -> system env -> hocon default;
   * Key of hocon file path: "POTAMOI_CONF_PATH".
   */
  def hoconSource(path: String): zio.config.ConfigSource = TypesafeConfigSource.fromTypesafeConfig(
    ZIO.attempt {
      val propConfigs    = ConfigFactory.systemProperties()
      val envConfigs     = ConfigFactory.systemEnvironment()
      val configFilePath = propConfigs.getStringOption(ConfigPathKey).orElse(envConfigs.getStringOption(ConfigPathKey))

      val fileConfigs = configFilePath match
        case Some(path) => ConfigFactory.parseFile(File(path))
        case None       => ConfigFactory.load()

      propConfigs
        .withFallback(envConfigs)
        .withFallback(fileConfigs)
        .getConfig(path)
    }
  )

  extension (config: Config)
    inline def getStringOption(path: String): Option[String] = {
      if !config.hasPath(path) then None
      else
        val value = config.getString(path).trim
        if value.nonEmpty then Some(value) else None
    }
