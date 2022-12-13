package potamoi.flink

import potamoi.zios.*
import potamoi.syntax.*

object FlinkRestRequestTest:

  val url = "http://10.233.46.104:8081"

  @main def testListJobsStatusInfo = flinkRest(url).listJobsStatusInfo.debugPretty.run

  @main def testListJobOverviewInfo = flinkRest(url).listJobOverviewInfo.debugPretty.run

  @main def testGetJobMetricsKeys = flinkRest(url).getJobMetricsKeys("e980c35c0b3da7c7b1c0a341979b20d5").debugPretty.run

  @main def testGetJmMetrics = flinkRest(url).getJmMetricsKeys.debugPretty.run
