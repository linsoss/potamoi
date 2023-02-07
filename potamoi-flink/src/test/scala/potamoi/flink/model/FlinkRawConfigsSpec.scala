package potamoi.flink.model

import org.scalatest.wordspec.AnyWordSpec
import potamoi.flink.model.deploy.{JmHaProp, MemProp}
import potamoi.syntax.*

class FlinkRawConfigsSpec extends AnyWordSpec:

  "dry config items" in {
    assert {
      JmHaProp(haImplClass = "", storageDir = "/tmp", clusterId = None).mapping == Map(
        "high-availability"            -> "",
        "high-availability.storageDir" -> "/tmp"
      )
    }
    assert {
      JmHaProp(haImplClass = "", storageDir = "/tmp", clusterId = Some("app-2")).mapping == Map(
        "high-availability"            -> "",
        "high-availability.storageDir" -> "/tmp",
        "high-availability.cluster-id" -> "app-2"
      )
    }
    assert {
      MemProp(jmMB = -2333, tmMB = 2333).mapping == Map(
        "jobmanager.memory.process.size"  -> "1920m",
        "taskmanager.memory.process.size" -> "2333m"
      )
    }
  }
