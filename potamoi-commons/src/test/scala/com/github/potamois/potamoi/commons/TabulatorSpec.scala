package com.github.potamois.potamoi.commons

import com.github.potamois.potamoi.testkit.STSpec

import scala.language.postfixOps

class TabulatorSpec extends STSpec {

  "Tabulator" should {

    "format a table 2" in {
      val table = Seq(
        Seq("f1", "f2", "f3", "f4", "f5", "f6"),
        Seq("d", 302.087, 133L, 2333, true, "hello world"),
        Seq("g", 3.231, 233L, 3342, false, "deep dark fantasy"),
      )
      val tableStr = Tabulator.format(table)
      println(tableStr)
    }

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
          || deep | 114514 |         |
          || dark |        | null    |
          ||      |        | fantasy |
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

    "format a table that contains cell with multiple line" in {
      val table = Seq(
        Seq("f1", "f2", "f3"),
        Seq(1, "line1", "line2"),
        Seq(2,
          """bala
            |balabalabalabala
            |balabala
            |""".stripMargin,
          """balabala
            |balabala
            |bala
            |balabalabalabalabalabalabalabalas""".stripMargin),
        Seq(3, "line3", "line4")
      )
      Tabulator.format(table, escapeJava = false) shouldBe
      """+----+------------------+-----------------------------------+
        || f1 | f2               | f3                                |
        |+----+------------------+-----------------------------------+
        || 1  | line1            | line2                             |
        || 2  | bala             | balabala                          |
        ||    | balabalabalabala | balabala                          |
        ||    | balabala         | bala                              |
        ||    |                  | balabalabalabalabalabalabalabalas |
        || 3  | line3            | line4                             |
        |+----+------------------+-----------------------------------+""".stripMargin
      Tabulator.format(table, escapeJava = true) shouldBe
      """+----+------------------------------------+-------------------------------------------------------------+
        || f1 | f2                                 | f3                                                          |
        |+----+------------------------------------+-------------------------------------------------------------+
        || 1  | line1                              | line2                                                       |
        || 2  | bala\nbalabalabalabala\nbalabala\n | balabala\nbalabala\nbala\nbalabalabalabalabalabalabalabalas |
        || 3  | line3                              | line4                                                       |
        |+----+------------------------------------+-------------------------------------------------------------+""".stripMargin
    }
  }

}
