package potamoi.kubernetes

import potamoi.syntax.*
import potamoi.zios.*

object K8sOperatorTest:

  val layer = K8sConf.default.asLayer >>> K8sOperator.live

  @main def testGetPodMetrics = zioRun {
    K8sOperator
      .getPodMetrics("app-t1-taskmanager-1-1", "fdev")
      .debugPretty
      .provide(layer)
  }

  @main def testGetDeploymentSpec = zioRun {
    K8sOperator
      .getDeploymentSpec("app-t1", "fdev")
      .debugPretty
      .provide(layer)
  }