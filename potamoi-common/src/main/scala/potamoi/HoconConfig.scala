package potamoi

import com.typesafe.config.{Config, ConfigFactory}
import zio.{Task, ULayer, ZIO, ZLayer}
import zio.config.typesafe.TypesafeConfigSource
import zio.ZIO.{attempt, logError}
import zio.config.ConfigSource

import java.io.File

object HoconConfig:

  val ConfigPathKey = "POTAMOI_CONF_PATH"

  /**
   * Load root Hocon formatted configuration.
   *
   * Priority: java properties -> system env -> hocon file;
   * File path value priority: java properties -> system env -> hocon default;
   * Key of hocon file path: "POTAMOI_CONF_PATH".
   */
  val live: ZLayer[Any, Throwable, Config] = ZLayer.fromZIO(load)

  /**
   * Create a empty hocon config layer.
   */
  val empty: ULayer[Config] = ZLayer.succeed(ConfigFactory.empty)

  /**
   * Create root hocon config layer manually.
   */
  def manual(config: Task[Config]): ZLayer[Any, Throwable, Config] = ZLayer.fromZIO(config)

  /**
   * Load sub config source from root hocon configuration.
   */
  def hoconSource(path: String): ZIO[Config, Nothing, ConfigSource] = {
    for {
      root      <- ZIO.service[Config]
      subConfig <- ZIO
                     .attempt(root.getConfig(path))
                     .tapError(_ => logError(s"Require config: $path"))
                     .orDie
      source     = TypesafeConfigSource.fromTypesafeConfig(ZIO.succeed(subConfig))
    } yield source
  }

  /**
   * Load sub config source from root hocon configuration directly.
   */
  def directHoconSource(path: String): ZIO[Any, Throwable, ConfigSource] =
    for {
      root      <- load
      subConfig <- ZIO.attempt(root.getConfig(path))
      source     = TypesafeConfigSource.fromTypesafeConfig(ZIO.succeed(subConfig))
    } yield source

  def load: Task[Config] = ZIO.attempt {
    val propConfigs    = ConfigFactory.systemProperties
    val envConfigs     = ConfigFactory.systemEnvironment
    val configFilePath = propConfigs.getStringOption(ConfigPathKey).orElse(envConfigs.getStringOption(ConfigPathKey))

    val fileConfigs = configFilePath match
      case Some(path) => ConfigFactory.parseFile(File(path))
      case None       => ConfigFactory.load()

    propConfigs
      .withFallback(envConfigs)
      .withFallback(fileConfigs)
  }

  extension (config: Config)
    inline def getStringOption(path: String): Option[String] = {
      if !config.hasPath(path) then None
      else
        val value = config.getString(path).trim
        if value.nonEmpty then Some(value) else None
    }
