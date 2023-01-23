package potamoi

import zio.config.magnolia.name

/**
 * Potamoi basic config
 */
case class BaseConf(@name("data-dir") dataDir: String = "var/potamoi")
