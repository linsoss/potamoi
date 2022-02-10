package com.github.potamois.potamoi.commons

import com.github.potamois.potamoi.commons.FutureImplicits._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Future, TimeoutException}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class FutureImplicitsSpec extends AnyWordSpec with Matchers {

  "FutureImplicit" should {

    "call the blocking waitResult method normally" in {
      val f = Future("hello")
      f.waitResult() shouldBe "hello"

      var a = 1
      val f2 = Future { a += 1; a }
      a shouldBe 1
      f2.waitResult() shouldBe 2
      a shouldBe 2
    }

    "call the blocking waitResult method normally with timeout param" in {
      val f = Future("hello")
      f.waitResult(3.seconds) shouldBe "hello"

      val f2 = Future { Thread.sleep(1000); 1 }
      f2.waitResult(3.seconds) shouldBe 1
    }

    "call the blocking waitResult method when timeout" in {
      val f = Future { Thread.sleep(10 * 1000); 1 }
      assertThrows[TimeoutException] {
        f.waitResult(1.seconds)
      }
    }

    "call the blocking waitResult method when an exception thrown from future" in {
      val f = Future(throw new RuntimeException("fail"))
      assertThrows[RuntimeException] {
        f.waitResult()
      }
    }

    "call the blocking waitResult method when an exception thrown from future and then catch it" in {
      var a = 1
      val f = Future {
        Thread.sleep(500)
        a += 1
        throw new Exception("test")
      }
      a shouldBe 1
      Try(f.waitResult()) match {
        case Failure(e) => e.getMessage shouldBe "test"
        case _ => fail("should be failure")
      }
      a shouldBe 2
    }

    "call the blocking tryWaitResult and catch the exception" in {
      def foo: Boolean => String = if (_) "pass" else throw new RuntimeException("fail")

      Future(foo(true)).tryWaitResult() match {
        case Success(s) => s shouldBe "pass"
        case Failure(_) => fail("should be success")
      }

      Future(foo(false)).tryWaitResult() match {
        case Success(_) => fail("should be failure")
        case Failure(e) => e.getMessage shouldBe "fail"
      }

      Future(Thread.sleep(1000)).tryWaitResult(200.milliseconds) match {
        case Success(_) => fail("should be timeout")
        case Failure(e) => e shouldBe a[TimeoutException]
      }
    }

  }
}
