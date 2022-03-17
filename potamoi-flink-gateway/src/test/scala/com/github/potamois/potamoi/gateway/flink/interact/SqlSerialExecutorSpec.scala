package com.github.potamois.potamoi.gateway.flink.interact

import akka.Done
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.github.potamois.potamoi.commons.FutureImplicits.sleep
import com.github.potamois.potamoi.gateway.flink.interact.SqlSerialExecutor._
import com.github.potamois.potamoi.testkit.akka.{STAkkaSpec, defaultConfig}
import org.apache.flink.table.api.SqlParserException

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

/**
 * Some of following cases would take a long time.
 *
 * What's more, when running remote flink cluster test cases, it requires a
 * flink cluster that already exists.
 */
class SqlSerialExecutorSpec extends ScalaTestWithActorTestKit(defaultConfig) with STAkkaSpec {

  // todo provide props via condition from hocon
  val props: ExecConfig = baseProps("remote")

  lazy val baseProps = Map(
    "local" -> ExecConfig.localEnv(
      rsCollectSt = RsCollectStrategy(EvictStrategy.DROP_TAIL, 25)),

    "remote" -> ExecConfig.remoteEnv(
      RemoteAddr("hs.assad.site", 32241),
      rsCollectSt = RsCollectStrategy(EvictStrategy.DROP_TAIL, 25))
  )

  lazy val logStmtRs = true

  def logSilent(content: String): Unit = if (logStmtRs) log.info(content)


  /**
   * Testing submit sql behavior of SqlSerialExecutor
   */
  "SqlSerialExecutor" should {

    "executes immediate sqls" in {
      val executor = spawn(SqlSerialExecutor("114514"))
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

      testProbeRef[Boolean](executor ! IsInProcess(_)).expectMessage(false)

      testProbeRef[RejectableDone] { ref =>
        executor ! ExecuteSqls(sqls, props, ref)
        testProbeRef[Boolean](executor ! IsInProcess(_)).expectMessage(true)
      }.expectMessage(30.seconds, Right(Done))

      testProbeRef[Option[SerialStmtsResult]](executor ! GetExecPlanRsSnapshot(_))
        .receiveMessagePf {
          case None => fail
          case Some(r) =>
            r.result.size shouldBe 2
            r.isFinished shouldBe true
            r.lastOpType shouldBe OpType.NORMAL
            r.allSuccess shouldBe true
            logSilent(r.toFriendlyString)
        }

      testProbeRef[Boolean](executor ! IsInProcess(_)).expectMessage(false)
    }


    "executes bounded insert sql" in {
      val executor = spawn(SqlSerialExecutor("114514"))
      val sqls =
        """create temporary table datagen_source (
          |    f_sequence int,
          |    f_random int,
          |    f_random_str string
          |  ) with (
          |    'connector' = 'datagen',
          |    'number-of-rows' = '10'
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

      testProbeRef[RejectableDone](executor ! ExecuteSqls(sqls, props, _)).expectMessage(30.seconds, Right(Done))

      testProbeRef[Option[SerialStmtsResult]](executor ! GetExecPlanRsSnapshot(_))
        .receiveMessagePf {
          case None => fail
          case Some(r) =>
            r.result.size shouldBe 3
            r.isFinished shouldBe true
            r.lastOpType shouldBe OpType.MODIFY
            r.allSuccess shouldBe true
            logSilent(r.toFriendlyString)
        }
    }


    "executes unbounded insert sql" in {
      val executor = spawn(SqlSerialExecutor("114514"))
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

      testProbeRef[RejectableDone] { ref =>
        executor ! ExecuteSqls(sqls, props, ref)
        sleep(4.seconds)
        executor ! CancelCurProcess
      }.expectMessage(30.seconds, Right(Done))

      testProbeRef[Option[SerialStmtsResult]](executor ! GetExecPlanRsSnapshot(_))
        .receiveMessagePf {
          case None => fail
          case Some(r) =>
            r.result.size shouldBe 3
            r.isFinished shouldBe true
            r.lastOpType shouldBe OpType.MODIFY
            r.allSuccess shouldBe true
            logSilent(r.toFriendlyString)
        }
    }


    "executes unbounded query sql" in {
      val executor = spawn(SqlSerialExecutor("114514"))
      val sqls =
        """create table datagen_source (
          |    f_sequence int,
          |    f_random int,
          |    f_random_str string
          |  ) with (
          |    'connector' = 'datagen'
          |  );
          |select * from datagen_source;
          |""".stripMargin

      val prop = props.copy(rsCollectSt = RsCollectStrategy(EvictStrategy.DROP_TAIL, 25))
      testProbeRef[RejectableDone](executor ! ExecuteSqls(sqls, prop, _)).expectMessage(30.seconds, Right(Done))
      // check SerialStmtsResult
      testProbeRef[Option[SerialStmtsResult]](executor ! GetExecPlanRsSnapshot(_))
        .receiveMessagePf {
          case None => fail
          case Some(r) =>
            r.result.size shouldBe 2
            r.isFinished shouldBe true
            r.lastOpType shouldBe OpType.QUERY
            r.allSuccess shouldBe true
            logSilent(r.toFriendlyString)
        }
      // check table result of query operation
      testProbeRef[Option[TableResultSnapshot]](executor ! GetQueryRsSnapshot(-1, _))
        .receiveMessagePf {
          case None => fail
          case Some(s) =>
            s.error shouldBe None
            s.isFinished shouldBe true
            s.data.cols shouldBe Seq(Column("f_sequence", "INT"), Column("f_random", "INT"), Column("f_random_str", "STRING"))
            s.data.rows.size shouldBe 25
            logSilent(s.data.tabulateContent)
        }
    }


    "executes bounded query sql" in {
      val executor = spawn(SqlSerialExecutor("114514"))
      val sqls =
        """create table datagen_source (
          |    f_sequence int,
          |    f_random int,
          |    f_random_str string
          |  ) with (
          |    'connector' = 'datagen'
          |  );
          |select * from datagen_source limit 10;
          |""".stripMargin
      val prop = props.copy(rsCollectSt = RsCollectStrategy(EvictStrategy.DROP_TAIL, 25))
      testProbeRef[RejectableDone](executor ! ExecuteSqls(sqls, prop, _)).expectMessage(30.seconds, Right(Done))

      testProbeRef[Option[TableResultSnapshot]](executor ! GetQueryRsSnapshot(-1, _))
        .receiveMessagePf {
          case None => fail
          case Some(s) =>
            s.error shouldBe None
            s.isFinished shouldBe true
            s.data.cols shouldBe Seq(Column("f_sequence", "INT"), Column("f_random", "INT"), Column("f_random_str", "STRING"))
            s.data.rows.size shouldBe 10
            logSilent(s.data.tabulateContent)
        }
    }


    "executes incorrect sqls plan" in {
      val executor = spawn(SqlSerialExecutor("114514"))
      val sqls =
        """create table datagen_source (
          |    f_sequence int,
          |    f_random int,
          |    f_random_str string,
          |  ) with (
          |    'connector' = 'datagen'
          |  );
          |select * from datagen_source limit 10;
          |""".stripMargin

      testProbeRef[RejectableDone](executor ! ExecuteSqls(sqls, props, _)).expectMessage(25.seconds, Right(Done))

      testProbeRef[Option[SerialStmtsResult]](executor ! GetExecPlanRsSnapshot(_)).receiveMessagePf {
        case None => fail
        case Some(r) =>
          r.result.size shouldBe 1
          r.isFinished shouldBe true
          r.lastOpType shouldBe OpType.UNKNOWN
          r.allSuccess shouldBe false
          r.result(0).rs match {
            case Right(_) => fail
            case Left(err) => err.stack.isInstanceOf[SqlParserException] shouldBe true
          }
      }
    }

  }


  /**
   * Testing result query command
   */
  "query result of query operation" ignore {
    "with limit size" in {
    }
    "with pageable param" in {
    }
  }

  /**
   * Testing result collection strategy
   */
  "result collection strategy" ignore {
    "DROP_TAIL evict strategy" in {

    }
    "DROP_HEAD evict strategy" in {

    }
  }

  /**
   * Testing subscribe result change events.
   */
  "subscribe result change" ignore {

  }

  /**
   * Testing request rejection while executor is already in querying process.
   */
  "query request rejection" ignore {}

}



