package com.github.potamois.potamoi.commons

import com.github.potamois.potamoi.commons.TryImplicits._
import com.github.potamois.potamoi.testkit.STSpec

import scala.language.postfixOps
import scala.util.Try

class TryImplicitsSpec extends STSpec {

  "foldIdentity" in {
    def foo(num: Int) = if (num > 5) throw new RuntimeException("foo err") else num

    def bar(num: Int): String = {
      val v = Try(foo(num)).foldIdentity { e =>
        println(s"bar catch: ${e.getMessage}")
        return "boom"
      }
      (v + 1).toString
    }

    def bar2(num: Int): String = {
      val v = Try(foo(num)).foldIdentity(return "boom")
      (v + 1).toString
    }

    bar(1) shouldBe "2"
    bar(6) shouldBe "boom"
    bar2(8) shouldBe "boom"
  }

}
