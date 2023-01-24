package potamoi.kubernetes

import potamoi.common.HoconConfig
import zio.{config, ULayer, ZLayer}
import zio.config.magnolia.descriptor
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Kubernetes configuration.
 */
case class K8sConf(
    debug: Boolean = false,
    namespace: Option[String] = Some("default"))

object K8sConf:

  val live: ZLayer[Any, Throwable, K8sConf] = ZLayer {
    for {
      source <- HoconConfig.directHoconSource("potamoi.k8s")
      config <- config.read(descriptor[K8sConf].from(source))
    } yield config
  }

  val default: ULayer[K8sConf] = ZLayer.succeed(K8sConf())
