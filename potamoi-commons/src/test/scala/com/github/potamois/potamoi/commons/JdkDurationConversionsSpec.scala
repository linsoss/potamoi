package com.github.potamois.potamoi.commons

import com.github.potamois.potamoi.testkit.STSpec

import java.time.{Duration => JdkDuration}
import scala.concurrent.duration.{Duration, DurationInt}

class JdkDurationConversionsSpec extends STSpec {

  import JdkDurationConversions._

  "java duration to scala duration" in {
    asJava(Duration.Zero) shouldBe JdkDuration.ZERO
    val duration: Duration = JdkDuration.ofSeconds(233)
    duration shouldBe 233.seconds
  }

  "scala duration to scala duration" in {
    asScala(JdkDuration.ZERO) shouldBe Duration.Zero
    val duration: JdkDuration = 233.seconds
    duration shouldBe JdkDuration.ofSeconds(233)
  }

}
