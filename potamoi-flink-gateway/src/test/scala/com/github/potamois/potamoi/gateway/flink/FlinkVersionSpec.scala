package com.github.potamois.potamoi.gateway.flink

import com.github.potamois.potamoi.testkit.STSpec
import org.apache.flink.runtime.execution.Environment

class FlinkVersionSpec extends STSpec {

  "FlinkVersion" should {

    "return the correct version of system" in {
      val currentVersion = classOf[Environment].getPackage.getImplementationVersion
      FlinkVersion.curSystemFlinkVers.version shouldBe currentVersion
    }

    "return the correct major version" in {
      var vers = new FlinkVersion("1.13.2")
      vers.major shouldBe "1.13"
      vers.majorSign shouldBe 113

      vers = new FlinkVersion("1.14.5")
      vers.major shouldBe "1.14"
      vers.majorSign shouldBe 114
    }
  }

}
