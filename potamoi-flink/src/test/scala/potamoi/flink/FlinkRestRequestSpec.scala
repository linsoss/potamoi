package potamoi.flink

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.DoNotDiscover
import potamoi.syntax.*
import potamoi.zios.*
import zio.ZIO

@DoNotDiscover
class FlinkRestRequestSpec extends AnyWordSpec:

  val url = "http://10.233.46.104:8081"

  "listJobsStatusInfo" in {
    flinkRest(url).listJobsStatusInfo.debugPretty.run
  }

  "listJobOverviewInfo" in {
    flinkRest(url).listJobOverviewInfo.debugPretty.run
  }

  "getJobMetricsKeys" in {
    flinkRest(url).getJobMetricsKeys("e980c35c0b3da7c7b1c0a341979b20d5").debugPretty.run
  }

  "getJmMetrics" in {
    flinkRest(url).getJmMetricsKeys.debugPretty.run
  }

  "isAvailable" in {
    flinkRest(url).isAvailable.debugPretty.run
  }

  "getDmDetail" in {
    flinkRest(url).getTaskManagerDetail("session-01-taskmanager-1-45").debugPretty.run
  }
