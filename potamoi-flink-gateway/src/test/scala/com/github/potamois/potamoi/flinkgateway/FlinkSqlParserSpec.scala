package com.github.potamois.potamoi.flinkgateway

import com.github.potamois.potamoi.flinkgateway.FlinkSqlParser.splitSqlStatement
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.language.postfixOps

class FlinkSqlParserSpec extends AnyWordSpec with Matchers {

  "FlinkSqlParser" when {

    "splitSqlStatement" should {

      "split normal sql content" in {
        val sql =
          """
            |  CREATE TABLE datagen_source (
            |    f_sequence INT,
            |    f_random INT,
            |    f_random_str STRING
            |  ) WITH (
            |    'connector' = 'datagen',
            |    'rows-per-second'= '25'
            |  );
            |
            |DESCRIBE datagen_source;
            |
            |show CATALOGS;
            |
            |explain SELECT * FROM datagen_source;
            |
            |""".stripMargin
        val extractedSqls = splitSqlStatement(sql)

        extractedSqls.size shouldBe 4
        extractedSqls shouldBe Seq(
          """CREATE TABLE datagen_source (
            |    f_sequence INT,
            |    f_random INT,
            |    f_random_str STRING
            |  ) WITH (
            |    'connector' = 'datagen'
            |    'rows-per-second'= '25'
            |  )""".stripMargin,
          "DESCRIBE datagen_source",
          "show CATALOGS",
          "explain SELECT * FROM datagen_source"
        )
      }

      "split empty sql content" in {
        splitSqlStatement(null) shouldBe Seq.empty
        splitSqlStatement("") shouldBe Seq.empty
        splitSqlStatement("   ") shouldBe Seq.empty
        splitSqlStatement("\n \n") shouldBe Seq.empty
        splitSqlStatement(";") shouldBe Seq.empty
        splitSqlStatement("  ;  ") shouldBe Seq.empty
        splitSqlStatement("  ;  ;  ") shouldBe Seq.empty
        splitSqlStatement("\n ; \n ; \n") shouldBe Seq.empty
      }

      "split sql content with single line comment using '//'" in {
        splitSqlStatement("// comment") shouldBe Seq.empty
        splitSqlStatement("// comment \n select * from table1") shouldBe Seq("select * from table1")
        splitSqlStatement("select * from table1 // comment ") shouldBe Seq("select * from table1")
        splitSqlStatement(
          """
            |// comment
            |//comment 2
            |  CREATE TABLE datagen_source (
            |    f_sequence INT,
            |    f_random INT,// comment 3
            |    f_random_str STRING// comment 4
            |  ) WITH (
            |    'connector' = 'datagen',
            |    'rows-per-second'= '25'
            |  );
            |
            |DESCRIBE datagen_source ;//comment 5
            |
            |// comment 6; comment 7
            |show CATALOGS // comment 8;
            |
            |////comment9
            |""".stripMargin) shouldBe Seq(

          """CREATE TABLE datagen_source (
            |    f_sequence INT,
            |    f_random INT,
            |    f_random_str STRING
            |  ) WITH (
            |    'connector' = 'datagen',
            |    'rows-per-second'= '25'
            |  )""".stripMargin,
          "DESCRIBE datagen_source",
          "show CATALOGS")
      }

      "split sql content with single line comment using '--'" in {
        splitSqlStatement("-- comment") shouldBe Seq.empty
        splitSqlStatement("-- comment \n select * from table1") shouldBe Seq("select * from table1")
        splitSqlStatement("select * from table1 -- comment ") shouldBe Seq("select * from table1")
        splitSqlStatement(
          """
            |-- comment
            |--comment 2
            |  CREATE TABLE datagen_source (
            |    f_sequence INT,
            |    f_random INT,-- comment 3
            |    f_random_str STRING-- comment 4
            |  ) WITH (
            |    'connector' = 'datagen',
            |    'rows-per-second'= '25'
            |  );
            |
            |DESCRIBE datagen_source ;--comment 5
            |
            |-- comment 6; comment 7
            |show CATALOGS -- comment 8;
            |
            |------- comment9
            |""".stripMargin) shouldBe Seq(

          """CREATE TABLE datagen_source (
            |    f_sequence INT,
            |    f_random INT,
            |    f_random_str STRING
            |  ) WITH (
            |    'connector' = 'datagen',
            |    'rows-per-second'= '25'
            |  )""".stripMargin,
          "DESCRIBE datagen_source",
          "show CATALOGS")
      }

      "split sql content with multiple lines comment using '/* */'" in {

      }
    }

  }

}
