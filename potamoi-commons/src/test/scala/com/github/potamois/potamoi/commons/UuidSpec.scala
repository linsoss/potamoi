package com.github.potamois.potamoi.commons

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class UuidSpec extends AnyWordSpec with Matchers{

  "Uuid" should {
    "genUUID" in {
      val uuid = Uuid.genUUID
      uuid.length shouldBe 36
    }
    "genShortUUID" in {
      val uuid = Uuid.genShortUUID
      uuid.length shouldBe 32
    }
  }

}
