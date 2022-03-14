package com.github.potamois.potamoi.flinkgateway

import akka.Done
import akka.actor.testkit.typed.scaladsl.{ScalaTestWithActorTestKit, TestProbe}
import com.github.potamois.potamoi.gateway.flink.{EvictStrategy, ExecConfig, RemoteAddr, RsCollectStrategy}
import com.github.potamois.potamoi.testkit.akka.STAkkaSpec

import scala.concurrent.duration.DurationInt

class ExptExecutorSpec extends ScalaTestWithActorTestKit with STAkkaSpec {

  import ExptExecutor._

  "Executor" should {

    val props = ExecConfig.remoteEnv(RemoteAddr("hs.assad.site", 32241),
      resultCollectStrategy=RsCollectStrategy(EvictStrategy.DROP_TAIL, 10))

    "test1" in {
      val executor = spawn(ExptExecutor("114514"))
      val sqls =
        """create temporary table datagen_source (
          |    f_sequence int,
          |    f_random int,
          |    f_random_str string
          |  ) with (
          |    'connector' = 'datagen'
          |  );
          |explain select * from datagen_source;
          |""".stripMargin
      val probe = TestProbe[Done]()
      executor ! ExecuteSqls(sqls, props, probe.ref)
      println(probe.receiveMessage(60.seconds))
    }

    "test2" in {
      val executor = spawn(ExptExecutor("114514"))
      val sqls =
        """create temporary table datagen_source (
          |    f_sequence int,
          |    f_random int,
          |    f_random_str string
          |  ) with (
          |    'connector' = 'datagen'
          |  );
          |create table print_table (
          |    f_sequence int,
          |    f_random int,
          |    f_random_str string
          |) with (
          |  'connector' = 'print'
          |);
          |insert into print_table select * from datagen_source;
          |""".stripMargin
      val probe = TestProbe[Done]()
      executor ! ExecuteSqls(sqls, props, probe.ref)
      probe.receiveMessage(60.seconds)
    }


    "test3" in {
      val executor = spawn(ExptExecutor("114514"))
      val sqls =
        """create temporary table datagen_source (
          |    f_sequence int,
          |    f_random int,
          |    f_random_str string
          |  ) with (
          |    'connector' = 'datagen'
          |  );
          |create table print_table (
          |    f_sequence int,
          |    f_random int,
          |    f_random_str string
          |) with (
          |  'connector' = 'print'
          |);
          |select * from datagen_source;
          |""".stripMargin
      val probe = TestProbe[Done]()
      executor ! ExecuteSqls(sqls, props, probe.ref)
      probe.receiveMessage(60.seconds)
    }

  }

}
