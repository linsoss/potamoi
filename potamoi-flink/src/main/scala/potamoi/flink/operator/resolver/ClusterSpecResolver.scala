package potamoi.flink.operator.resolver

import potamoi.flink.model.deploy.ClusterSpec

/**
 * Flink cluster definition resolver for [[ClusterSpec]].
 */
object ClusterSpecResolver {

  /**
   * Flink raw configuration keys that are not allowed to be customized by users.
   */
  lazy val notAllowCustomFlinkConfigKeys: Vector[String] = Vector(
    "execution.target",
    "kubernetes.cluster-id",
    "kubernetes.namespace",
    "kubernetes.container.image", 
    
    "kubernetes.service-account",
    "kubernetes.jobmanager.service-account",
    "kubernetes.pod-template-file",
    "kubernetes.pod-template-file.taskmanager",
    "kubernetes.pod-template-file.jobmanager",
    "$internal.deployment.config-dir",
    "pipeline.jars",
    "$internal.application.main",
    "$internal.application.program-args",
  )
}
