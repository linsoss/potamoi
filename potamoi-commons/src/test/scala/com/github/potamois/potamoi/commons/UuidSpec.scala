package com.github.potamois.potamoi.commons

import com.github.potamois.potamoi.testkit.STSpec

class UuidSpec extends STSpec {

  "Uuid" should {

    "genUUID36" in {
      val uuid = Uuid.genUUID36
      uuid.length shouldBe 36
    }

    "genUUID32" in {
      val uuid = Uuid.genUUID32
      uuid.length shouldBe 32
    }

    "genUUID16" in {
      val uuid = Uuid.genUUID16
      uuid.length shouldBe 16
    }

    "36 bytes uuids should not be repeated" in {
      val uuids = (1 to 1000).map(_ => Uuid.genUUID36)
      uuids.distinct.length shouldBe uuids.length
    }

    "32 bytes uuids should not be repeated" in {
      val uuids = (1 to 1000).map(_ => Uuid.genUUID32)
      uuids.distinct.length shouldBe uuids.length
    }

    "16 bytes uuids should not be repeated" in {
      val uuids = (1 to 1000).map(_ => Uuid.genUUID16)
      uuids.distinct.length shouldBe uuids.length
    }

  }

}
