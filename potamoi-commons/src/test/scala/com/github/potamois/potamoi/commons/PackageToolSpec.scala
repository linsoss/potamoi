package com.github.potamois.potamoi.commons

import com.github.potamois.potamoi.testkit.STSpec

import scala.collection.mutable
import scala.util.Try

/**
 * Test case for [[com.github.potamois.potamoi.commons]] package object.
 */
class PackageToolSpec extends STSpec {

  "RichString" should {
    "compact string" in {
      val string =
        """  CREATE TABLE datagen_source (
          |    f_sequence INT,
          |    f_random INT,
          |    f_random_str STRING
          |  ) WITH (
          |    'connector' = 'datagen',
          |    'rows-per-second' = '25'
          |  );""".stripMargin

      string.compact shouldBe "CREATE TABLE datagen_source ( f_sequence INT, f_random INT, f_random_str STRING ) WITH ( 'connector' = 'datagen', 'rows-per-second' = '25' );"
    }
  }

  "RichTry" should {
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


  "RichThrowable" should {
    "getStackTraceAsString" in {
      val e = new RuntimeException("boom")
      e.getStackTraceAsString should startWith("java.lang.RuntimeException: boom")
    }
    "getStackTraceAsString from null Throwable object" in {
      val e: Throwable = null
      e.getStackTraceAsString shouldBe ""
    }
  }


  "RichMap" should {
    "softSet for Immutable Map" in {
      val map = Map("a" -> 1, "b" -> 2)
      map ?+ ("a" -> 3) shouldBe Map("a" -> 1, "b" -> 2)
      map ?+ ("c" -> 3) shouldBe Map("a" -> 1, "b" -> 2, "c" -> 3)
    }
    "softSet for Mutable Map" in {
      var map = mutable.Map("a" -> 1, "b" -> 2)
      map ?+= "a" -> 3
      map shouldBe Map("a" -> 1, "b" -> 2)
      map ?+= "c" -> 3
      map shouldBe Map("a" -> 1, "b" -> 2, "c" -> 3)

      // chain
      map = mutable.Map("a" -> 1, "b" -> 2)
      map ?+= "a" -> 3 ?+= "c" -> 3 += "d" -> 5
      map shouldBe Map("a" -> 1, "b" -> 2, "c" -> 3, "d" -> 5)
    }
  }

}
