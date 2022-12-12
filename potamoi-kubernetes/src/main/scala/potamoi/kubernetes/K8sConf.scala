package potamoi.kubernetes

import zio.ZLayer
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Kubernetes configuration.
 */
case class K8sConf(debug: Boolean = false)

object K8sConf:
  given JsonCodec[K8sConf] = DeriveJsonCodec.gen[K8sConf]
  val default              = ZLayer.succeed(K8sConf())
