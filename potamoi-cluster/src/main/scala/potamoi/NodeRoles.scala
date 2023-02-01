package potamoi

/**
 * potamoi system cluster node roles
 */
object NodeRoles:

  /**
   * Flink module core services, including tracking of flink on k8s clusters.
   */
  val flinkService = "flink-svc"

  /**
   * Flink sql interactive gateway service, the marVer parameter is the major version
   * number of flink, e.g. "flink-intr-116" for version 1.16.
   */
  def flinkInterpreter(majorVerSeq: Int) = s"flink-intr-$majorVerSeq"
