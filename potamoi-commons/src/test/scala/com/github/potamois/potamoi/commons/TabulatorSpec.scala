package com.github.potamois.potamoi.commons

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.language.postfixOps

class TabulatorSpec extends AnyWordSpec with Matchers {

  "Tabulator" should {

    "format a table" in {
      val table = Seq(
        Seq("f1", "f2", "f3", "f4", "f5", "f6"),
        Seq("d", 302.087, 133L, 2333, true, "hello world"),
        Seq("g", 3.231, 233L, 3342, false, "deep dark fantasy"),
      )
      val tableStr = Tabulator.format(table)
      // println(tableStr)
      val expected =
        """+----+---------+-----+------+-------+-------------------+
          || f1 | f2      | f3  | f4   | f5    | f6                |
          |+----+---------+-----+------+-------+-------------------+
          || d  | 302.087 | 133 | 2333 | true  | hello world       |
          || g  | 3.231   | 233 | 3342 | false | deep dark fantasy |
          |+----+---------+-----+------+-------+-------------------+""".stripMargin
      tableStr shouldBe expected
    }

    "format a table contains null values" in {
      val table = Seq(
        Seq("f1", "f2", "f3"),
        Seq("deep", 114514, null),
        Seq("dark", null, "null"),
        Seq(null, null, "fantasy")
      )
      val tableStr = Tabulator.format(table)
      // println(tableStr)
      val expected =
        """+------+--------+---------+
          || f1   | f2     | f3      |
          |+------+--------+---------+
          || deep | 114514 | null    |
          || dark | null   | null    |
          || null | null   | fantasy |
          |+------+--------+---------+""".stripMargin
      tableStr shouldBe expected
    }

    "format a null table" in {
      Tabulator.format(null) shouldBe ""
      Tabulator.format(Seq.empty) shouldBe ""
      Tabulator.format(Seq(Seq.empty, Seq.empty))
    }

    "format a table that rows of varying length" in {
      Tabulator.format(Seq(Seq("a", "b", "c"), Seq("d", "e", "f"), Seq("g", "h", "i", "233"))) shouldBe ""
      Tabulator.format(Seq(Seq("a", "b", "c"), Seq.empty)) shouldBe ""
    }
  }

}
