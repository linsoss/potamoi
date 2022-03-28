package com.github.potamois.potamoi.gateway.flink.interact

import akka.Done
import akka.actor.testkit.typed.scaladsl.{ScalaTestWithActorTestKit, TestProbe}
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.Behaviors
import com.github.potamois.potamoi.commons.EitherAlias.success
import com.github.potamois.potamoi.commons.FutureImplicits.sleep
import com.github.potamois.potamoi.gateway.flink.PageReq
import com.github.potamois.potamoi.gateway.flink.interact.EvictStrategy._
import com.github.potamois.potamoi.gateway.flink.interact.FsiExecutor._
import com.github.potamois.potamoi.gateway.flink.interact.ExecRsChangeEvent.{AcceptStmtsExecPlanEvent, AllStmtsDone}
import com.github.potamois.potamoi.testkit.akka.{STAkkaSpec, defaultConfig}
import org.apache.flink.table.api.SqlParserException
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

/**
 * Some of following cases would take a long time.
 *
 * What's more, when running remote flink cluster test cases, it requires a
 * flink cluster that already exists.
 *
 * @author Al-assad
 */
class FsiSerialExecutorSpec extends ScalaTestWithActorTestKit(defaultConfig) with STAkkaSpec {

  // todo provide props via condition from hocon
  // Executor Config
  val props: ExecProps = baseProps("remote")

  lazy val baseProps = Map(
    "local" -> ExecProps.localEnv(rsCollectSt = DROP_TAIL -> 25),
    "remote" -> ExecProps.remoteEnv(remoteAddr = "hs.assad.site" -> 32241, rsCollectSt = DROP_TAIL -> 25)
  )

  // whether log statements result
  lazy val logStmtRs = true
  def logSilent(content: String): Unit = if (logStmtRs) log.info(content)

  def newExecutor: ActorRef[Command] = spawn(FsiSerialExecutor("114514"))

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

      probeRef[ExecutionPlanResult](executor ! GetExecPlanRsSnapshot(_))
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

      executor ! SubscribeState(spawn(ExecRsChangeEventPrinter("114514")))
      probeRef[RejectableDone](executor ! ExecuteSqls(sqls, props, _)).expectMessage(30.seconds, Right(Done))

      probeRef[ExecutionPlanResult](executor ! GetExecPlanRsSnapshot(_))
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

      probeRef[ExecutionPlanResult](executor ! GetExecPlanRsSnapshot(_))
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
      probeRef[ExecutionPlanResult](executor ! GetExecPlanRsSnapshot(_))
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
      probeRef[QueryResult](executor ! GetQueryRsSnapshot(-1, _))
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

      probeRef[QueryResult](executor ! GetQueryRsSnapshot(-1, _))
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

      probeRef[ExecutionPlanResult](executor ! GetExecPlanRsSnapshot(_)).receivePF {
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

      probeRef[QueryResult](executor ! GetQueryRsSnapshot(8, _)).receivePF {
        case None => fail
        case Some(rs) => rs.data.rows.size shouldBe 8
      }
      probeRef[QueryResult](executor ! GetQueryRsSnapshot(100, _)).receivePF {
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
      probeRef[PageQueryResult] { ref =>
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
      probeRef[PageQueryResult](executor ! GetQueryRsSnapshotByPage(PageReq(40, 10), _)).receivePF {
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
      probeRef[PageQueryResult](executor ! GetQueryRsSnapshotByPage(PageReq(3, 8), _)).receivePF {
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

      probeRef[QueryResult](executor ! GetQueryRsSnapshot(-1, _)).receivePF {
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

      testProbe[QueryResult] { probe =>
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
      val executor = spawn(FsiSerialExecutor("114514"))
      val prop = props.copy(rsCollectSt = DROP_TAIL -> 30)
      executor ! SubscribeState(spawn(ExecRsChangeEventPrinter("114514", printEachRowReceived = true)))
      probeRef[RejectableDone](executor ! ExecuteSqls(sql, prop, _)) expectMessage(30.seconds, Right(Done))
    }

    "custom subscriber" in {
      val executor = spawn(FsiSerialExecutor("114514"))
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
      val cusSubscribeActor = spawn(Behaviors.setup[ExecRsChangeEvent] { _ =>
        Behaviors.receiveMessage {
          case AcceptStmtsExecPlanEvent(stmts, _) => stmts.size shouldBe 2; Behaviors.same
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
  "cancel/accept/reject/terminate control behavior" should {

    val sql =
      """create table datagen_source (
        |    f_sequence int,
        |    f_random int,
        |    f_random_str string
        |  ) with (
        |    'connector' = 'datagen',
        |     'rows-per-second'='3'
        |  );
        |select * from datagen_source;
        |""".stripMargin

    val prop = props.copy(rsCollectSt = DROP_HEAD -> 10)

    "rejects another execution plan when the current plan is running" in {
      val executor = newExecutor
      executor ! SubscribeState(spawn(ExecRsChangeEventPrinter("114514", printEachRowReceived = true)))
      val probe = TestProbe[RejectableDone]
      executor ! ExecuteSqls(sql, prop, probe.ref)
      probeRef[RejectableDone](executor ! ExecuteSqls(sql, prop, _)) receivePF {
        case Left(reject) => reject.isInstanceOf[BusyInProcess] shouldBe true
        case _ => fail
      }
      executor ! CancelCurProcess
    }

    "cancel the current execution plan process" in {
      val executor = newExecutor
      executor ! SubscribeState(spawn(ExecRsChangeEventPrinter("114514")))
      executor ! ExecuteSqls(sql, prop, system.ignoreRef)
      executor ! CancelCurProcess
      val probe = TestProbe[Boolean]()
      executor ! IsInProcess(probe.ref)
      probe.expectMessage(false)
      executor ! CancelCurProcess
    }

    "test execute -> cancel -> execute process" in {
      val executor = newExecutor
      executor ! SubscribeState(spawn(ExecRsChangeEventPrinter("114514")))
      // execute
      executor ! ExecuteSqls(sql, prop, system.ignoreRef)
      // be rejected
      probeRef[RejectableDone](executor ! ExecuteSqls(sql, prop, _)) receivePF {
        case Left(reject) =>
          reject.isInstanceOf[BusyInProcess] shouldBe true
          log.info(reject.asInstanceOf[BusyInProcess].reason)
        case _ => fail
      }
      probeRef[Boolean](executor ! IsInProcess(_)).expectMessage(true)
      // cancel
      executor ! CancelCurProcess
      probeRef[Boolean](executor ! IsInProcess(_)).expectMessage(false)
      // execute
      executor ! ExecuteSqls(sql, prop, system.ignoreRef)
      probeRef[Boolean](executor ! IsInProcess(_)).expectMessage(true)
      executor ! CancelCurProcess
    }

    "terminal the current executor when the executor is busy" in {
      val watch = createTestProbe[Command]()
      val executor = newExecutor
      executor ! SubscribeState(spawn(ExecRsChangeEventPrinter("114514")))
      executor ! ExecuteSqls(sql, prop, system.ignoreRef)
      executor ! Terminate()
      watch.expectTerminated(executor)
    }

    "terminal the current executor when the executor is idle" in {
      val watch = createTestProbe[Command]()
      val executor = newExecutor
      executor ! SubscribeState(spawn(ExecRsChangeEventPrinter("114514")))
      executor ! Terminate()
      watch.expectTerminated(executor)
    }

  }


  /**
   * Testing execute sql statements with different dependencies.
   *
   * todo classify with Tag
   *
   * @note This test case is currently suitable for flink-14.
   */
  "execute plan with udf/dependencies" should {

    val fakerDepPath = getClass.getResource("/flink-faker-0.4.1.jar").getPath
    val udfDepPath = getClass.getResource("/flink-udf-example-1.0.jar").getPath

    "with third-dependencies" in {
      val executor = newExecutor
      val prop = props.copy(flinkDeps = Seq(fakerDepPath), rsCollectSt = DROP_TAIL -> 10)
      val sql =
        """create temporary table heros (
          | `name1` string,
          | `age1` int,
          | `age2` int
          |) with (
          | 'connector' = 'faker',
          | 'fields.name1.expression' = '#{superhero.name}',
          | 'fields.age1.expression' = '#{number.numberBetween ''0'',''1000''}',
          | 'fields.age2.expression' = '#{number.numberBetween ''0'',''1000''}'
          |);
          |select name1, age1, age2 from heros;
          |""".stripMargin
      // executor ! SubscribeState(spawn(RsEventChangePrinter("114514")))
      probeRef[RejectableDone](executor ! ExecuteSqls(sql, prop, _)) expectMessage(60.seconds, success(Done))
      probeRef[ExecutionPlanResult](executor ! GetExecPlanResult(_)) receivePF {
        case None => fail
        case Some(rs) =>
          rs.allSuccess shouldBe true
          rs.result.size shouldBe 2
          rs.lastOpType shouldBe OpType.QUERY
          rs.isFinished shouldBe true
      }
      probeRef[QueryResult](executor ! GetQueryResult(-1, _)) receivePF {
        case None => fail
        case Some(rs) =>
          rs.error shouldBe None
          rs.isFinished shouldBe true
          rs.data.cols.map(_.name) shouldBe Seq("name1", "age1", "age2")
          rs.data.rows.size shouldBe 10
      }
    }

    "with udf" in {
      val executor = newExecutor
      val prop = props.copy(flinkDeps = Seq(udfDepPath), rsCollectSt = DROP_TAIL -> 10)
      val sql =
        """create table datagen_source (
          |    f1 int,
          |    f2 int,
          |    f3 string
          |  ) with (
          |    'connector' = 'datagen'
          |  );
          |create temporary function my_sum as 'com.github.al.assad.udf.SumFunction';
          |select f1, f2, f3, my_sum(f1,f2) as f12 from datagen_source;
          |""".stripMargin
      // executor ! SubscribeState(spawn(RsEventChangePrinter("114514")))
      probeRef[RejectableDone](executor ! ExecuteSqls(sql, prop, _)) expectMessage(60.seconds, success(Done))
      probeRef[ExecutionPlanResult](executor ! GetExecPlanResult(_)) receivePF {
        case None => fail
        case Some(rs) =>
          rs.allSuccess shouldBe true
          rs.result.size shouldBe 3
          rs.lastOpType shouldBe OpType.QUERY
          rs.isFinished shouldBe true
      }
      probeRef[QueryResult](executor ! GetQueryResult(-1, _)) receivePF {
        case None => fail
        case Some(rs) =>
          rs.error shouldBe None
          rs.isFinished shouldBe true
          rs.data.cols.map(_.name) shouldBe Seq("f1", "f2", "f3", "f12")
          rs.data.rows.size shouldBe 10
      }
    }

    "with both third-dependencies and udf" in {
      val executor = newExecutor
      val prop = props.copy(flinkDeps = Seq(udfDepPath, fakerDepPath), rsCollectSt = DROP_TAIL -> 10)
      val sql =
        """create temporary table heros (
          | `name1` string,
          | `age1` int,
          | `age2` int
          |) with (
          | 'connector' = 'faker',
          | 'fields.name1.expression' = '#{superhero.name}',
          | 'fields.age1.expression' = '#{number.numberBetween ''0'',''1000''}',
          | 'fields.age2.expression' = '#{number.numberBetween ''0'',''1000''}'
          |);
          |create temporary function my_sum as 'com.github.al.assad.udf.SumFunction';
          |select name1, age1, age2, my_sum(age1, age2) as sum_age from heros;
          |""".stripMargin
      // executor ! SubscribeState(spawn(RsEventChangePrinter("114514")))
      probeRef[RejectableDone](executor ! ExecuteSqls(sql, prop, _)) expectMessage(60.seconds, success(Done))
      probeRef[ExecutionPlanResult](executor ! GetExecPlanResult(_)) receivePF {
        case None => fail
        case Some(rs) =>
          rs.allSuccess shouldBe true
          rs.result.size shouldBe 3
          rs.lastOpType shouldBe OpType.QUERY
          rs.isFinished shouldBe true
      }
      probeRef[QueryResult](executor ! GetQueryResult(-1, _)) receivePF {
        case None => fail
        case Some(rs) =>
          rs.error shouldBe None
          rs.isFinished shouldBe true
          rs.data.cols.map(_.name) shouldBe Seq("name1", "age1", "age2", "sum_age")
          rs.data.rows.size shouldBe 10
      }

    }
  }

}
