package com.github.potamois.potamoi.gateway.flink

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.github.potamois.potamoi.commons.FutureImplicits.sleep
import com.github.potamois.potamoi.testkit.akka.STAkkaSpec

import scala.concurrent.duration.DurationInt

class SqlSerialExecutorSpec extends ScalaTestWithActorTestKit with STAkkaSpec {

  import SqlSerialExecutor._

  "SqlSerialExecutor" should {

    val props = ExecConfig.remoteEnv(RemoteAddr("hs.assad.site", 32241))

    "execute immediate queries correctly" in {
      val executor = spawn(SqlSerialExecutor("114514", props))
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
      testProbe[Either[ExecReject, SerialStmtsResult]] { probe =>
        executor ! ExecuteSqls(sqls, probe.ref)
        val result = probe.receiveMessage
        result match {
          case Left(_) => fail
          case Right(data) =>
            data.result.size shouldBe 2
            data.result.exists(_.rs.isLeft) shouldBe false
        }
        println(result)
      }
    }

    "execute modify operations correctly" in {
      val executor = spawn(SqlSerialExecutor("114514", props))
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
      testProbe[Either[ExecReject, SerialStmtsResult]] { probe =>
        executor ! ExecuteSqls(sqls, probe.ref)
        val result = probe.receiveMessage
        result match {
          case Left(_) => fail
          case Right(data) =>
            //            data.result.size shouldBe 3
            //            data.result.exists(_.rs.isLeft) shouldBe false
            //            data.result.last.rs shouldBe Right(SubmitModifyOpDone)
        }
        println(result)
      }

      sleep(10.seconds)
      //      testProbe[Boolean] { probe =>
      //        eventually {
      //          executor ! IsTrackStmtFinished(probe.ref)
      //          probe.expectMessage(true)
      //        }
      //      }

      testProbe[Option[TrackStmtResult]] { probe =>
        executor ! GetTrackStmtResult(probe.ref)
        val re = probe.receiveMessage(10.seconds)
        re.isDefined shouldBe true
        println(re)
        re.get.result match {
          case Right(data) =>
          case Left(err) => err.printStackTrace()
        }
      }

    }


  }

}
