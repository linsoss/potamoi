package potamoi.flink.model

import potamoi.syntax.*

class FlinkRawConfigsSpec extends munit.FunSuite:

  test("dry config items") {
    assert {
      JmHaConfig(haImplClz = "", storageDir = "/tmp", clusterId = None).mapping == Map(
        "high-availability"            -> "",
        "high-availability.storageDir" -> "/tmp"
      )
    }
    assert {
      JmHaConfig(haImplClz = "", storageDir = "/tmp", clusterId = Some("app-2")).mapping == Map(
        "high-availability"            -> "",
        "high-availability.storageDir" -> "/tmp",
        "high-availability.cluster-id" -> "app-2"
      )
    }
    assert {
      MemConfig(jmMB = -2333, tmMB = 2333).mapping == Map(
        "jobmanager.memory.process.size"  -> "1920m",
        "taskmanager.memory.process.size" -> "2333m"
      )
    }
  }
