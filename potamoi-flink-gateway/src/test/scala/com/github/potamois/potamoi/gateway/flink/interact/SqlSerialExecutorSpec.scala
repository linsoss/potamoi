package com.github.potamois.potamoi.gateway.flink.interact

import akka.Done
import akka.actor.testkit.typed.scaladsl.{ScalaTestWithActorTestKit, TestProbe}
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.Behaviors
import com.github.potamois.potamoi.commons.FutureImplicits.sleep
import com.github.potamois.potamoi.gateway.flink.PageReq
import com.github.potamois.potamoi.gateway.flink.interact.EvictStrategy._
import com.github.potamois.potamoi.gateway.flink.interact.ResultChangeEvent.{AcceptStmtsExecPlan, AllStmtsDone}
import com.github.potamois.potamoi.gateway.flink.interact.SqlSerialExecutor._
import com.github.potamois.potamoi.testkit.akka.{STAkkaSpec, defaultConfig}
import org.apache.flink.table.api.SqlParserException
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}

import scala.concurrent.duration.DurationInt

/**
 * Some of following cases would take a long time.
 *
 * What's more, when running remote flink cluster test cases, it requires a
 * flink cluster that already exists.
 */
class SqlSerialExecutorSpec extends ScalaTestWithActorTestKit(defaultConfig) with STAkkaSpec {

  // todo provide props via condition from hocon
  // Executor Config
  val props: ExecConfig = baseProps("remote")

  lazy val baseProps = Map(
    "local" -> ExecConfig.localEnv(rsCollectSt = DROP_TAIL -> 25),
    "remote" -> ExecConfig.remoteEnv(remoteAddr = "hs.assad.site" -> 32241, rsCollectSt = DROP_TAIL -> 25)
  )

  // whether log statements result
  lazy val logStmtRs = true
  def logSilent(content: String): Unit = if (logStmtRs) log.info(content)

  def newExecutor: ActorRef[Command] = spawn(SqlSerialExecutor("114514"))

  /**
   * Testing submit sql behavior of SqlSerialExecutor
   */
  "SqlSerialExecutor" should {

    "executes immediate sqls" in {
      val executor = newExecutor
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

      probeRef[Boolean](executor ! IsInProcess(_)).expectMessage(false)

      probeRef[RejectableDone] { ref =>
        executor ! ExecuteSqls(sqls, props, ref)
        probeRef[Boolean](executor ! IsInProcess(_)).expectMessage(true)
      }.expectMessage(30.seconds, Right(Done))

      probeRef[Option[SerialStmtsResult]](executor ! GetExecPlanRsSnapshot(_))
        .receivePF {
          case None => fail
          case Some(r) =>
            r.result.size shouldBe 2
            r.isFinished shouldBe true
            r.lastOpType shouldBe OpType.NORMAL
            r.allSuccess shouldBe true
            logSilent(r.toFriendlyString)
        }

      probeRef[Boolean](executor ! IsInProcess(_)).expectMessage(false)
    }

    "executes bounded insert sql" in {
      val executor = newExecutor
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

      probeRef[RejectableDone](executor ! ExecuteSqls(sqls, props, _)).expectMessage(30.seconds, Right(Done))

      probeRef[Option[SerialStmtsResult]](executor ! GetExecPlanRsSnapshot(_))
        .receivePF {
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
      val executor = newExecutor
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

      probeRef[RejectableDone] { ref =>
        executor ! ExecuteSqls(sqls, props, ref)
        sleep(5.seconds)
        executor ! CancelCurProcess
      }.expectMessage(30.seconds, Right(Done))

      probeRef[Option[SerialStmtsResult]](executor ! GetExecPlanRsSnapshot(_))
        .receivePF {
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
      val executor = newExecutor
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

      probeRef[RejectableDone] { ref =>
        executor ! ExecuteSqls(sqls, props.copy(rsCollectSt = DROP_TAIL -> 25), ref)
      }.expectMessage(30.seconds, Right(Done))
      // check SerialStmtsResult
      probeRef[Option[SerialStmtsResult]](executor ! GetExecPlanRsSnapshot(_))
        .receivePF {
          case None => fail
          case Some(r) =>
            r.result.size shouldBe 2
            r.isFinished shouldBe true
            r.lastOpType shouldBe OpType.QUERY
            r.allSuccess shouldBe true
            logSilent(r.toFriendlyString)
        }
      // check table result of query operation
      probeRef[Option[TableResultSnapshot]](executor ! GetQueryRsSnapshot(-1, _))
        .receivePF {
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
      val executor = newExecutor
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
      probeRef[RejectableDone] {
        executor ! ExecuteSqls(sqls, props.copy(rsCollectSt = DROP_TAIL -> 25), _)
      }.expectMessage(30.seconds, Right(Done))

      probeRef[Option[TableResultSnapshot]](executor ! GetQueryRsSnapshot(-1, _))
        .receivePF {
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
      val executor = newExecutor
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

      probeRef[RejectableDone](executor ! ExecuteSqls(sqls, props, _)).expectMessage(25.seconds, Right(Done))

      probeRef[Option[SerialStmtsResult]](executor ! GetExecPlanRsSnapshot(_)).receivePF {
        case None => fail
        case Some(r) =>
          r.result.size shouldBe 1
          r.isFinished shouldBe true
          r.lastOpType shouldBe OpType.UNKNOWN
          r.allSuccess shouldBe false
          r.result.head.rs match {
            case Right(_) => fail
            case Left(err) => err.stack.isInstanceOf[SqlParserException] shouldBe true
          }
      }
    }

  }


  /**
   * Testing result query command
   */
  "query result of query operation" should {

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

    "with row limit size" in {
      val executor = newExecutor
      val prop = props.copy(rsCollectSt = DROP_TAIL -> 30)

      probeRef[RejectableDone] {
        executor ! ExecuteSqls(sqls, prop, _)
      }.expectMessage(30.seconds, Right(Done))

      probeRef[Option[TableResultSnapshot]](executor ! GetQueryRsSnapshot(8, _)).receivePF {
        case None => fail
        case Some(rs) => rs.data.rows.size shouldBe 8
      }
      probeRef[Option[TableResultSnapshot]](executor ! GetQueryRsSnapshot(100, _)).receivePF {
        case None => fail
        case Some(rs) => rs.data.rows.size shouldBe 30
      }
    }

    "with pageable param" in {
      val executor = newExecutor
      val prop = props.copy(rsCollectSt = DROP_TAIL -> 30)
      probeRef[RejectableDone] {
        executor ! ExecuteSqls(sqls, prop, _)
      }.expectMessage(30.seconds, Right(Done))

      // normal page
      probeRef[Option[PageableTableResultSnapshot]] { ref =>
        executor ! GetQueryRsSnapshotByPage(PageReq(0, 10), ref)
      }.receivePF {
        case None => fail
        case Some(rs) =>
          rs.error shouldBe None
          rs.isFinished shouldBe true
          val pageRs = rs.data
          pageRs.index shouldBe 0
          pageRs.size shouldBe 10
          pageRs.totalPages shouldBe 3
          pageRs.totalRows shouldBe 30
          pageRs.hasNext shouldBe true
          pageRs.data.cols shouldBe Seq(Column("f_sequence", "INT"), Column("f_random", "INT"), Column("f_random_str", "STRING"))
          pageRs.data.rows.size shouldBe 10
      }

      // over page size
      probeRef[Option[PageableTableResultSnapshot]](executor ! GetQueryRsSnapshotByPage(PageReq(40, 10), _)).receivePF {
        case None => fail
        case Some(rs) =>
          rs.error shouldBe None
          rs.isFinished shouldBe true
          val pageRs = rs.data
          pageRs.index shouldBe 40
          pageRs.size shouldBe 0
          pageRs.totalPages shouldBe 3
          pageRs.totalRows shouldBe 30
          pageRs.hasNext shouldBe false
          pageRs.data.cols shouldBe Seq(Column("f_sequence", "INT"), Column("f_random", "INT"), Column("f_random_str", "STRING"))
          pageRs.data.rows.size shouldBe 0
      }

      // last page
      probeRef[Option[PageableTableResultSnapshot]](executor ! GetQueryRsSnapshotByPage(PageReq(3, 8), _)).receivePF {
        case None => fail
        case Some(rs) =>
          rs.error shouldBe None
          rs.isFinished shouldBe true
          val pageRs = rs.data
          pageRs.index shouldBe 3
          pageRs.size shouldBe 6
          pageRs.totalPages shouldBe 4
          pageRs.totalRows shouldBe 30
          pageRs.hasNext shouldBe false
          pageRs.data.rows.size shouldBe 6
      }
    }
  }


  /**
   * Testing result collection strategy
   */
  "result collection strategy" should {

    val sql1 =
      """create table datagen_source (
        |    f_sequence int,
        |    f_random int,
        |    f_random_str string
        |  ) with (
        |    'connector' = 'datagen'
        |  );
        |select * from datagen_source;
        |""".stripMargin

    val sql2 =
      """create table datagen_source (
        |    f_sequence int,
        |    f_random int,
        |    f_random_str string
        |  ) with (
        |    'connector' = 'datagen'
        |  );
        |select * from datagen_source limit 10;
        |""".stripMargin

    def testCollectStrategy(st: RsCollectStrategy, sql: String, expectedRowSize: Int): Unit = {
      val executor = newExecutor
      val prop = props.copy(rsCollectSt = st)

      probeRef[RejectableDone](executor ! ExecuteSqls(sql, prop, _))
        .expectMessage(30.seconds, Right(Done))

      probeRef[Option[TableResultSnapshot]](executor ! GetQueryRsSnapshot(-1, _)).receivePF {
        case None => fail
        case Some(rs) => rs.data.rows.size shouldBe expectedRowSize
      }
    }

    "DROP_TAIL evict strategy" in {
      testCollectStrategy(DROP_TAIL -> 20, sql1, 20)
    }
    "DROP_HEAD evict strategy" in {
      val executor = newExecutor
      val prop = props.copy(rsCollectSt = DROP_HEAD -> 10)
      executor ! ExecuteSqls(sql1, prop, system.ignoreRef)

      testProbe[Option[TableResultSnapshot]] { probe =>
        eventually(Timeout(25.seconds), Interval(1.seconds)) {
          executor ! GetQueryRsSnapshot(-1, probe.ref)
          probe.receiveMessage().get.data.rows.size shouldBe 10
        }
      }

      testProbe[Boolean] { probe =>
        executor ! IsInProcess(probe.ref)
        probe.expectMessage(true)
        executor ! CancelCurProcess
        eventually(Timeout(5.seconds), Interval(50.millis)) {
          executor ! IsInProcess(probe.ref)
          probe.expectMessage(false)
        }
      }
    }
    "DROP_TAIL evict strategy for query statement with limit rex" in {
      testCollectStrategy(DROP_TAIL -> 20, sql2, 10)
    }
    "DROP_HEAD evict strategy for query statement with limit rex" in {
      testCollectStrategy(DROP_HEAD -> 20, sql2, 10)
    }
  }

  /**
   * Testing subscribe result change events.
   */
  "subscribe result change" should {

    val sql =
      """create table datagen_source (
        |    f_sequence int,
        |    f_random int,
        |    f_random_str string
        |  ) with (
        |    'connector' = 'datagen'
        |  );
        |select * from datagen_source;
        |""".stripMargin

    "testing RsEventChangePrinter" in {
      val executor = spawn(SqlSerialExecutor("114514"))
      val prop = props.copy(rsCollectSt = DROP_TAIL -> 30)
      executor ! SubscribeState(spawn(RsEventChangePrinter("114514", printEachRowReceived = true)))
      probeRef[RejectableDone](executor ! ExecuteSqls(sql, prop, _)) expectMessage(30.seconds, Right(Done))
    }

    "custom subscriber" in {
      val executor = spawn(SqlSerialExecutor("114514"))
      val prop = props.copy(rsCollectSt = DROP_TAIL -> 30)

      sealed trait Cmd
      case class Set(signal: Boolean) extends Cmd
      case class Get(reply: ActorRef[Boolean]) extends Cmd
      val trashActor = spawn(Behaviors.setup[Cmd] { _ =>
        var signal = false
        Behaviors.receiveMessage {
          case Set(v) => signal = v; Behaviors.same
          case Get(reply) => reply ! signal; Behaviors.same
        }
      })
      val cusSubscribeActor = spawn(Behaviors.setup[ResultChange] { _ =>
        Behaviors.receiveMessage {
          case AcceptStmtsExecPlan(stmts) => stmts.size shouldBe 2; Behaviors.same
          case AllStmtsDone(rs) => trashActor ! Set(true); Behaviors.same
          case _ => Behaviors.same
        }
      })

      executor ! SubscribeState(cusSubscribeActor)
      executor ! ExecuteSqls(sql, prop, system.ignoreRef)
      executor ! CancelCurProcess
      eventually {
        probeRef[Boolean](trashActor ! Get(_)) expectMessage true
      }
    }
  }


  /**
   * Testing request rejection while executor is already in querying process.
   */
  "cancel/accept/reject control behavior" should {

    val sql =
      """create table datagen_source (
        |    f_sequence int,
        |    f_random int,
        |    f_random_str string
        |  ) with (
        |    'connector' = 'datagen'
        |  );
        |select * from datagen_source;
        |""".stripMargin

    val prop = props.copy(rsCollectSt = DROP_HEAD -> 10)

    "rejects another execution plan when the current plan is running" in {
      val executor = newExecutor
      executor ! ExecuteSqls(sql, prop, system.ignoreRef)
      probeRef[RejectableDone](executor ! ExecuteSqls(sql, prop, _)) receivePF {
        case Left(reject) => reject.isInstanceOf[BusyInProcess] shouldBe true
        case _ => fail
      }
    }

    "cancel the current execution plan process" in {
      val executor = newExecutor
      executor ! ExecuteSqls(sql, prop, system.ignoreRef)
      executor ! CancelCurProcess
      val probe = TestProbe[Boolean]()
      executor ! IsInProcess(probe.ref)
      probe.expectMessage(false)
      executor ! CancelCurProcess
    }

    "test execute -> cancel -> execute process" in {
      val executor = newExecutor
      // execute
      executor ! ExecuteSqls(sql, prop, system.ignoreRef)
      // be rejected
      probeRef[RejectableDone](executor ! ExecuteSqls(sql, prop, _)) receivePF {
        case Left(reject) =>
          reject.isInstanceOf[BusyInProcess] shouldBe true
          log.info(reject.asInstanceOf[BusyInProcess].reason)
        case _ => fail
      }
      // todo bug can not receive cancel command
      //      probeRef[Boolean](executor ! IsInProcess(_)).expectMessage(true)
      //      // cancel
      sleep(5.seconds)
      executor ! CancelCurProcess
      sleep(10.seconds)
      //      probeRef[Boolean](executor ! IsInProcess(_)).expectMessage(false)
      //      // execute
      //      executor ! ExecuteSqls(sql, prop, system.ignoreRef)
      //      probeRef[Boolean](executor ! IsInProcess(_)).expectMessage(true)
      //      executor ! CancelCurProcess
    }

  }

}



