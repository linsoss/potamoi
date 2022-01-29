package com.github.potamois.potamoi.commons

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.language.postfixOps

class UsingSpec extends AnyWordSpec with Matchers {

  class Resource extends AutoCloseable {
    var isClosed = false
    var isConsumed = false
    def consume(): Unit = isConsumed = true
    @throws[Exception] def forceThrowException(): Unit = throw new Exception("handling resource failures")
    override def close(): Unit = isClosed = true
  }

  object Resource {
    @throws[Exception] def failCreate(): Resource = throw new Exception("resource creation failed")
  }

  "Using" should {

    "auto close single resource" in {
      val resource = new Resource
      Using(resource) { rs => rs.consume() }
      resource.isClosed shouldBe true
      resource.isConsumed shouldBe true
    }

    "auto close multiple resources" in {
      val resource1 = new Resource
      val resource2 = new Resource
      Using.Manager { use =>
        val rs1 = use(resource1)
        val rs2 = use(resource2)
        rs1.consume()
        rs2.consume()
      }
      resource1.isClosed shouldBe true
      resource1.isConsumed shouldBe true
      resource2.isClosed shouldBe true
      resource2.isConsumed shouldBe true
    }

    "error interception" in {
      assertThrows[Exception] {new Resource().forceThrowException()}
      val resource = new Resource
      val re = Using(resource) { rs =>
        rs.consume()
        rs.forceThrowException()
      }
      re.isFailure shouldBe true
      re.failed.get.getMessage shouldBe "handling resource failures"
      resource.isClosed shouldBe true
      resource.isConsumed shouldBe true
    }

    "error interception of Using.Manager" in {
      val resource1 = new Resource
      val resource2 = new Resource
      val re = Using.Manager { use =>
        val rs1 = use(resource1)
        val rs2 = use(resource2)
        rs1.consume()
        rs1.forceThrowException()
        rs2.consume()
      }
      re.isFailure shouldBe true
      resource1.isClosed shouldBe true
      resource2.isClosed shouldBe true
      resource1.isConsumed shouldBe true
      resource2.isConsumed shouldBe false
    }

    "fail to create resource" in {
      assertThrows[Exception] {Resource.failCreate()}
      val re = Using(Resource.failCreate()) { rs => rs.consume() }
      re.isFailure shouldBe true
      re.failed.get.getMessage shouldBe "resource creation failed"
    }
  }

}
