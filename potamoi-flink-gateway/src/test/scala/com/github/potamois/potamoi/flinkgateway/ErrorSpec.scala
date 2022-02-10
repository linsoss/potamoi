package com.github.potamois.potamoi.flinkgateway

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ErrorSpec extends AnyWordSpec with Matchers {

  "Error" should {

    "be created from a message and Throwable" in {
      Error("test", new Exception("exception stack")) shouldBe Error("test", "exception stack")
    }

    "be created from a message" in {
      Error("test") shouldBe Error("test", "")
    }
  }

}
