package potamoi.kubernetes.model

import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Kubernetes pod metrics.
 */
case class PodMetrics(timestamp: Long, containers: Vector[ContainerMetrics] = Vector.empty)

/**
 * cpu unit: m,  memory unit: Ki
 */
case class ContainerMetrics(name: String, cpu: K8sQuantity, memory: K8sQuantity)

object PodMetrics:
  given JsonCodec[ContainerMetrics] = DeriveJsonCodec.gen[ContainerMetrics]
  given JsonCodec[PodMetrics]       = DeriveJsonCodec.gen[PodMetrics]
