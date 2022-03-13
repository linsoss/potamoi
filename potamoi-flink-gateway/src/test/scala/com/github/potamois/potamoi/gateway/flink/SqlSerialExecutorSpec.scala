package com.github.potamois.potamoi.gateway.flink

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.github.potamois.potamoi.testkit.akka.STAkkaSpec

class SqlSerialExecutorSpec extends ScalaTestWithActorTestKit with STAkkaSpec {

  import SqlSerialExecutor._

  "SqlSerialExecutor" should {

    val props = ExecConfig.localEnv()

    "execute immediate queries correctly" in {
      val executor = spawn(SqlSerialExecutor("114514", props))
      val sqls =
        """create temporary table datagen_source (
          |    f_sequence int,
          |    f_random int,
          |    f_random_str string,
          |    ts as localtimestamp,
          |    watermark for ts as ts
          |  ) with (
          |    'connector' = 'datagen',
          |    'rows-per-second'= '25',
          |    'fields.f_sequence.kind'='sequence',
          |    'fields.f_sequence.start'='1',
          |    'fields.f_sequence.end'='500',
          |    'fields.f_random.min'='1',
          |    'fields.f_random.max'='500',
          |    'fields.f_random_str.length'='10',
          |    'number-of-rows' = '100'
          |  );
          |explain select * from datagen_source;
          |""".stripMargin
    }
  }

}
