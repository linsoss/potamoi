package com.github.potamois.potamoi.commons

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Potamoi hocon configuration
 *
 * @author Al-assad
 */
object PotaConfig {

  // potamoi hocon resource name
  val resourceName = "potamoi-reference"

  // root hocon config object of potamoi
  val root: Config = ConfigFactory.load().withFallback(ConfigFactory.load(resourceName))

  implicit class RichConfig(config: Config) {
    // ensure that the config has been fallback with potamoi resource
    def ensurePotamoi: Config = config.withFallback(ConfigFactory.load(resourceName))
  }

}
