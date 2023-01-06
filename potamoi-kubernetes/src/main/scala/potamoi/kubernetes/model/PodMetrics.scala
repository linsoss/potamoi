package potamoi.kubernetes.model

import zio.json.JsonCodec

/**
 * Kubernetes pod metrics.
 */
case class PodMetrics(timestamp: Long, containers: Vector[ContainerMetrics] = Vector.empty) derives JsonCodec

/**
 * cpu unit: m,  memory unit: Ki
 */
case class ContainerMetrics(name: String, cpu: K8sQuantity, memory: K8sQuantity) derives JsonCodec
