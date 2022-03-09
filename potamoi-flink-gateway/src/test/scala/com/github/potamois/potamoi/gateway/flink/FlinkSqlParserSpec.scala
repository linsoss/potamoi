package com.github.potamois.potamoi.gateway.flink

import com.github.potamois.potamoi.testkit.STSpec

import scala.language.postfixOps

class FlinkSqlParserSpec extends STSpec {

  "FlinkSqlParser" when {

    "extractSqlStatements" should {
      def extract = FlinkSqlParser.extractSqlStatements _

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
        val extractedSqls = extract(sql)

        extractedSqls.size shouldBe 4
        extractedSqls shouldBe Seq(
          """CREATE TABLE datagen_source (
            |    f_sequence INT,
            |    f_random INT,
            |    f_random_str STRING
            |  ) WITH (
            |    'connector' = 'datagen',
            |    'rows-per-second'= '25'
            |  )""".stripMargin,
          "DESCRIBE datagen_source",
          "show CATALOGS",
          "explain SELECT * FROM datagen_source"
        )
      }

      "split empty sql content" in {
        extract(null) shouldBe Seq.empty
        extract("") shouldBe Seq.empty
        extract("   ") shouldBe Seq.empty
        extract("\n \n") shouldBe Seq.empty
        extract(";") shouldBe Seq.empty
        extract("  ;  ") shouldBe Seq.empty
        extract("  ;  ;  ") shouldBe Seq.empty
        extract("\n ; \n ; \n") shouldBe Seq.empty
      }

      "split sql content with single line comment using '//'" in {
        extract("// comment") shouldBe Seq.empty
        extract("// comment \n select * from table1") shouldBe Seq("select * from table1")
        extract("select * from table1 // comment ") shouldBe Seq("select * from table1")
        extract(
          """
            |// comment
            |   //comment 2
            |  CREATE TABLE datagen_source (
            |    f_sequence INT,
            |    f_random INT,  // comment 3
            |    f_random_str STRING // comment 4
            |  ) WITH (
            |  // comment 10
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
            |""".stripMargin) shouldBe
          Seq(
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
        extract("-- comment") shouldBe Seq.empty
        extract("-- comment \n select * from table1") shouldBe Seq("select * from table1")
        extract("select * from table1 -- comment ") shouldBe Seq("select * from table1")
        extract(
          """
            |-- comment
            |   --comment 2
            |  CREATE TABLE datagen_source (
            |    f_sequence INT,
            |    f_random INT, -- comment 3
            |    f_random_str STRING  -- comment 4
            |  ) WITH (
            |  -- comment 10
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
            |""".stripMargin) shouldBe
          Seq(
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
        extract("/* comment */") shouldBe Seq.empty
        extract("/* comment \n comment \n */") shouldBe Seq.empty
        extract("/* comment \n comment */ select * from table1") shouldBe Seq("select * from table1")
        extract("select * from table1 /* comment \n comment */ ") shouldBe Seq("select * from table1")

        extract(
          """
            |/* comment 1 */
            |/*comment2*/
            |CREATE TABLE datagen_source (
            |    f_sequence INT,
            |    f_random INT,  /* comment3 */
            |    f_random_str STRING
            |  ) WITH (
            |  /* comment 4
            |      comment 4 */
            |    'connector' = 'datagen',
            |    'rows-per-second'= '25'  /* comment 5
            |         comment 5
            |    */
            |  );
            |
            |/* comment 6
            |   comment 6
            |   comment 6 */
            |DESCRIBE datagen_source;
            |
            |show CATALOGS; /* comment 7
            |comment 7
            | */
            |
            |explain SELECT * FROM datagen_source /* comment 8 */;
            |/**/
            |""".stripMargin) shouldBe
          Seq(
            """CREATE TABLE datagen_source (
              |    f_sequence INT,
              |    f_random INT,
              |    f_random_str STRING
              |  ) WITH (
              |    'connector' = 'datagen',
              |    'rows-per-second'= '25'
              |  )""".stripMargin,
            "DESCRIBE datagen_source",
            "show CATALOGS",
            "explain SELECT * FROM datagen_source")
      }

      "split sql content with multiple lines comment using unclosed '/* */" in {
        extract(
          """
            |/* comment 1 */
            |DESCRIBE datagen_source;
            |  /* comment2
            |  show CATALOGS;
            |""".stripMargin) shouldBe Seq.empty

        extract(
          """
            |DESCRIBE datagen_source;
             comment2 */
            |  show CATALOGS;
            |""".stripMargin) shouldBe Seq.empty
      }

      "split sql content with comments of mixed use '//', '--' and '/**/'" in {
        extract(
          """ /* comment 1
            |  comment 1 */
            | -- comment 2
            |// comment 3
            |CREATE TABLE datagen_source (
            |    f_sequence INT, -- comment 4
            |    f_random INT,  // comment 5
            |    f_random_str STRING
            |  ) WITH (
            |  /* comment 5
            |      comment 5 */
            |    'connector' = 'datagen',
            |    'rows-per-second'= '25'
            |  );
            |
            |/* comment 6 */
            |DESCRIBE datagen_source;
            |
            |show CATALOGS; -- comment 7
            |
            |explain SELECT * FROM datagen_source // comment 8
            |
            |/* // comment9 -- comment10 */
            |-- comment 11 // comment 12
            | /* comment 13
            |   -- comment 14
            |     // comment 15
            |*/
            |""".stripMargin) shouldBe
          Seq(
            """CREATE TABLE datagen_source (
              |    f_sequence INT,
              |    f_random INT,
              |    f_random_str STRING
              |  ) WITH (
              |    'connector' = 'datagen',
              |    'rows-per-second'= '25'
              |  )""".stripMargin,
            "DESCRIBE datagen_source",
            "show CATALOGS",
            "explain SELECT * FROM datagen_source")
      }
    }

  }

}
