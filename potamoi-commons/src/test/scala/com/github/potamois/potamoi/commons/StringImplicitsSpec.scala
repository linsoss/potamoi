package com.github.potamois.potamoi.commons

import com.github.potamois.potamoi.commons.StringImplicits.StringWrapper
import com.github.potamois.potamoi.testkit.STSpec

class StringImplicitsSpec extends STSpec {

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
