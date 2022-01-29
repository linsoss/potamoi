package com.github.potamois.potamoi.commons

import org.scalatest.wordspec.AnyWordSpec

class UuidSpec extends AnyWordSpec {

  "Uuid" should {
    "genUUID" in {
      val uuid = Uuid.genUUID
      assert(uuid.length == 36)
    }
    "genShortUUID" in {
      val uuid = Uuid.genShortUUID
      assert(uuid.length == 32)
    }
  }

}
