package com.github.potamois.potamoi.gateway.flink.interact

import akka.Done
import com.github.potamois.potamoi.gateway.flink.interact.FsiExecutor.{ExecPlanResult, QueryResult, MaybeDone}
import com.github.potamois.potamoi.testkit.STSpec

/**
 * Quick Flink sql test and expected cases.
 *
 * @author Al-assad
 */
//noinspection TypeAnnotation
object QuickSqlCases extends STSpec {

  val selectSqls = SqlPlanExpect(
    sql =
      """create table datagen_source (
        |    f1 int,
        |    f2 double,
        |    f3 string
        |  ) with (
        |    'connector' = 'datagen'
        |  );
        |select * from datagen_source limit 10;
        |""".stripMargin,
    passGetExecPlanResult = expectGetExecPlanResult(r => r.lastOpType shouldBe OpType.QUERY),
    passGetQueryResult = expectHasGetQueryResult { r =>
      r.cols shouldBe Seq("f1", "f2", "f3")
      r.rows.nonEmpty shouldBe true
    }
  )

  val explainSqls = SqlPlanExpect(
    sql =
      """create table datagen_source (
        |    f1 int,
        |    f2 double,
        |    f3 string
        |  ) with (
        |    'connector' = 'datagen'
        |  );
        |explain select * from datagen_source;
        |""".stripMargin,
    passGetExecPlanResult = expectGetExecPlanResult(r => r.lastOpType shouldBe OpType.NORMAL),
    passGetQueryResult = expectNonGetQueryResult
  )


  case class SqlPlanExpect(sql: String,
                           passExecuteSqls: MaybeDone => Unit = expectExecuteSqls,
                           passGetExecPlanResult: ExecPlanResult => Unit = expectGetExecPlanResult(_ => ()),
                           passGetQueryResult: QueryResult => Unit
                          )

  private def expectExecuteSqls: MaybeDone => Unit = {
    case Left(_) => fail
    case Right(r) => r shouldBe Done
  }

  private def expectGetExecPlanResult(addition: SerialStmtsResult => Unit): ExecPlanResult => Unit = {
    case None => fail
    case Some(r) =>
      r.isFinished shouldBe true
      r.allSuccess shouldBe true
      addition(r)
  }

  private def expectNonGetQueryResult: QueryResult => Unit = { re =>
    re shouldBe None
  }

  private def expectHasGetQueryResult(addition: TableResultData => Unit): QueryResult => Unit = {
    case None => fail
    case Some(r) =>
      r.error shouldBe None
      addition(r.data)
  }
}
