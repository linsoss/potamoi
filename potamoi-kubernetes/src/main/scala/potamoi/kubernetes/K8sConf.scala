package potamoi.kubernetes

import zio.ZLayer
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Kubernetes configuration.
 */
case class K8sConf(
    debug: Boolean = false,
    namespace: Option[String] = Some("default"))
    derives JsonCodec

object K8sConf:
  val default = K8sConf()
