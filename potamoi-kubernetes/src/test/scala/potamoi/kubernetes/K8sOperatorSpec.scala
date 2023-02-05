package potamoi.kubernetes

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{DoNotDiscover, Ignore}
import potamoi.syntax.*
import potamoi.zios.*

@DoNotDiscover
class K8sOperatorSpec extends AnyWordSpec:

  val layer = K8sConf.default >>> K8sOperator.live

  "getPodMetrics" in zioRun {
    K8sOperator
      .getPodMetrics("app-t1-taskmanager-1-1", "fdev")
      .debugPretty
      .provide(layer)
  }

  "getDeploymentSpec" in zioRun {
    K8sOperator
      .getDeploymentSpec("app-t1", "fdev")
      .debugPretty
      .provide(layer)
  }
