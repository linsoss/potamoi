package com.github.potamois.potamoi.commons

import com.github.potamois.potamoi.testkit.STSpec

import java.time.{Duration => JdkDuration}
import scala.concurrent.duration.{Duration, DurationInt, SECONDS}

class JdkDurationConversionsSpec extends STSpec {

  import JdkDurationConversions._

  "java duration to scala duration" in {
    toScala(JdkDuration.ZERO) shouldBe Duration.Zero

    val duration: Duration = JdkDuration.ofSeconds(233)
    duration shouldBe 233.seconds

    toScala(JdkDuration.ofMillis(1024), SECONDS) shouldBe 1.seconds
  }

  "scala duration to scala duration" in {
    toJava(Duration.Zero) shouldBe JdkDuration.ZERO

    val duration: JdkDuration = 233.seconds
    duration shouldBe JdkDuration.ofSeconds(233)
  }

}
