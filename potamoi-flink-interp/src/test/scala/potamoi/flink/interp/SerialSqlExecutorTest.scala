package potamoi.flink.interp

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.Ignore
import potamoi.common.ZIOExtension.zioRun
import potamoi.flink.interp.model.{QuerySqlRs, RemoteClusterEndpoint, ResultStoreConf, SessionDef}
import potamoi.flink.interp.model.RemoteClusterEndpoint.given
import potamoi.flink.interp.model.ResultDropStrategy.DropTail
import potamoi.flink.model.FlinkRuntimeMode.{Batch, Streaming}
import potamoi.flink.model.FlinkTargetType
import potamoi.flink.model.FlinkTargetType.{Local, Remote}
import potamoi.fs.refactor.RemoteFsOperator
import potamoi.fs.S3FsBackendConfDev
import potamoi.fs.refactor.backend.S3FsBackend
import potamoi.logger.PotaLogger
import potamoi.syntax.toPrettyStr
import potamoi.zios.*
import potamoi.PotaErr
import potamoi.flink.interp.FlinkInterpErr.BeCancelled
import zio.{durationInt, IO, Schedule, Scope, Task, ZIO}
import zio.Console.printLine
import zio.ZIO.{executor, logErrorCause, logInfo, sleep}

//@Ignore
class SerialSqlExecutorTest extends AnyWordSpec:

  def testing[E, A](sessDef: SessionDef)(f: SerialSqlExecutor => ZIO[Scope, E, A]) =
    (for {
      fs       <- ZIO.service[RemoteFsOperator]
      executor <- ZIO.succeed(SerialSqlExecutorImpl("114514", sessDef, fs))
      _        <- executor.start
      rs       <- f(executor)
      _        <- executor.stop
    } yield rs)
      .provide(S3FsBackendConfDev.asLayer >>> S3FsBackend.live, Scope.default)
      .provideLayer(PotaLogger.default)
      .run

  val dataGenTableSql = """CREATE TABLE Orders (
                          |    order_number BIGINT,
                          |    price        DECIMAL(32,2),
                          |    buyer        ROW<first_name STRING, last_name STRING>,
                          |    mset        MULTISET<STRING>,
                          |    order_time   TIMESTAMP(3)
                          |) WITH (
                          |  'connector' = 'datagen',
                          |  'number-of-rows' = '20'
                          |)
                          |""".stripMargin

  val dataGenEndlessTableSql = """CREATE TABLE Orders (
                                 |    order_number BIGINT,
                                 |    price        DECIMAL(32,2),
                                 |    buyer        ROW<first_name STRING, last_name STRING>,
                                 |    mset        MULTISET<STRING>,
                                 |    order_time   TIMESTAMP(3)
                                 |) WITH (
                                 |  'connector' = 'datagen',
                                 |  'rows-per-second'='2'
                                 |)
                                 |""".stripMargin

  val dataFakerTableSql = """CREATE TABLE Heros (
                            |  h_name STRING,
                            |  h_power STRING,
                            |  h_age INT
                            |) WITH (
                            |  'connector' = 'faker',
                            |  'fields.h_name.expression' = '#{superhero.name}',
                            |  'fields.h_power.expression' = '#{superhero.power}',
                            |  'fields.h_power.null-rate' = '0.05',
                            |  'fields.h_age.expression' = '#{number.numberBetween ''0'',''1000''}'
                            |);
                            |""".stripMargin

  "execute sql normally on local" in testing(SessionDef.local()) { executor =>
    for {
      _                 <- executor.submitSql(dataGenTableSql).debugPretty
      _                 <- executor.submitSql("explain select * from Orders").debugPretty
      _                 <- executor.submitSql("show catalogs").debugPretty
      _                 <- executor.submitSql("show tables").debugPretty
      desc              <- executor.submitSql("select * from Orders").debugPretty.map { case r: QuerySqlRs => r.handleId -> r.dataWatchStream }
      _                 <- printLine("receive result stream")
      (handleId, stream) = desc
      _                 <- stream.foreach(row => printLine(row.show))
      _                 <- printLine("retrieve result page")
      _                 <- executor.retrieveResultPage(handleId, 1, 20).debugPretty
      _                 <- printLine("list handleId")
      _                 <- executor.listHandleId.debugPretty
      _                 <- printLine("list handle status")
      _                 <- executor.listHandleStatus.debugPretty
      _                 <- printLine("list handle frame")
      _                 <- executor.listHandleFrame.debugPretty
    } yield ()
  }

  "execute sql normally on remote" in testing(SessionDef.remote(execMode = Streaming, endpoint = "10.233.62.91" -> 8081)) { executor =>
    for {
      _                 <- executor.submitSql(dataGenTableSql).debugPretty
      _                 <- executor.submitSql("explain select * from Orders").debugPretty
      _                 <- executor.submitSql("show catalogs").debugPretty
      _                 <- executor.submitSql("show tables").debugPretty
      desc              <- executor.submitSql("select * from Orders").debugPretty.map { case r: QuerySqlRs => r.handleId -> r.dataWatchStream }
      _                 <- printLine("receive result stream")
      (handleId, stream) = desc
      _                 <- stream.foreach(row => printLine(row.show))
      _                 <- printLine("retrieve result page")
      _                 <- executor.retrieveResultPage(handleId, 1, 20).debugPretty
    } yield ()
  }

  "complete sql" in testing(SessionDef.local()) { executor =>
    for {
      _ <- executor.submitSql(dataGenTableSql).debugPretty
      _ <- logInfo("hint 1") *> executor.completeSql("select * from ").debugPretty
      _ <- logInfo("hint 2") *> executor.completeSql("select *  Orders", 8).debugPretty
    } yield ()
  }

  "execute sql with extra jars on local" in testing(
    SessionDef.local(
      execMode = Streaming,
      jars = List("pota://flink-faker-0.5.1.jar"),
      resultStore = ResultStoreConf(20, DropTail)
    )) { executor =>
    for {
      _      <- executor.submitSql(dataFakerTableSql).debugPretty
      stream <- executor.submitSql("select * from Heros").debugPretty.map { case r: QuerySqlRs => r.dataWatchStream }
      _      <- stream.foreach(row => printLine(row.show))
    } yield ()
  }

  "execute sql with extra jars on remote" in testing(
    SessionDef.remote(
      endpoint = "10.233.62.91" -> 8081,
      execMode = Streaming,
      localJars = List("pota://flink-faker-0.5.1.jar"),
      clusterJars = List("pota://flink-faker-0.5.1.jar"),
      resultStore = ResultStoreConf(20, DropTail)
    )) { executor =>
    for {
      _      <- executor.submitSql(dataFakerTableSql).debugPretty
      stream <- executor.submitSql("select * from Heros").debugPretty.map { case r: QuerySqlRs => r.dataWatchStream }
      _      <- stream.foreach(row => printLine(row.show))
    } yield ()
  }

  "execute add jar sql on local" in testing(
    SessionDef.local(resultStore = ResultStoreConf(10, DropTail))
  ) { executor =>
    for {
      _ <- executor.submitSql("add jar 'pota://flink-faker-0.5.1.jar';").debugPretty
      _ <- executor.submitSql("show jars").debugPretty
      _ <- executor.submitSql(dataFakerTableSql).debugPretty
      _ <- executor
             .submitSql("select * from Heros;")
             .debugPretty
             .map { case r: QuerySqlRs => r.dataWatchStream }
             .flatMap { stream =>
               stream.foreach(row => printLine(row.show))
             }
    } yield ()
  }

  "execute add jar sql on remote" in testing(
    SessionDef.remote(
      endpoint = "10.233.62.91" -> 8081,
      execMode = Streaming,
      resultStore = ResultStoreConf(20, DropTail)
    )) { executor =>
    for {
      _ <- executor.submitSql("add jar 'pota://flink-faker-0.5.1.jar';").debugPretty
      _ <- executor.submitSql("show jars").debugPretty
      _ <- executor.submitSql(dataFakerTableSql).debugPretty
      _ <- executor
             .submitSql("select * from Heros;")
             .debugPretty
             .map { case r: QuerySqlRs => r.dataWatchStream }
             .flatMap { stream =>
               stream.foreach(row => printLine(row.show))
             }
    } yield ()
  }

  "cancel handle on local" in testing(SessionDef.local()) { executor =>
    for {
      _ <- executor.submitSql(dataGenEndlessTableSql).debugPretty
      _ <- executor
             .submitSql("select * from Orders")
             .debugPretty
             .map { case r: QuerySqlRs => r.dataWatchStream }
             .flatMap { stream =>
               stream.foreach(e => printLine("collect row: " + e.kind.shortString()))
             }
             .fork
      _ <- executor
             .submitSql("show tables")
             .debugPretty
             .fork
      _ <- (printLine("cancel executor!") *> executor.cancel).delay(20.seconds)
      _ <- printLine("let me see:")
      _ <- executor.listHandleFrame.debugPretty
    } yield ()
  }

  "cancel handle on remote" in testing(SessionDef.remote(execMode = Streaming, endpoint = "10.233.62.91" -> 8081)) { executor =>
    for {
      _ <- executor.submitSql(dataGenEndlessTableSql).debugPretty
      _ <- executor
             .submitSql("select * from Orders")
             .debugPretty
             .map { case r: QuerySqlRs => r.dataWatchStream }
             .flatMap { stream =>
               stream.foreach(e => printLine("collect row: " + e.kind.shortString()))
             }
             .fork
      _ <- executor
             .submitSql("show tables")
             .debugPretty
             .fork
      _ <- (printLine("cancel executor!") *> executor.cancel).delay(20.seconds)
      _ <- printLine("let me see:")
      _ <- executor.listHandleFrame.debugPretty
    } yield ()
  }

  "execute some illegal sql" in testing(SessionDef.local()) { executor =>
    for {
      f1 <- executor.submitSql(dataGenTableSql).fork
      f2 <- executor.submitSql("explain select * from Orders").delay(1.millis).fork
      f3 <- executor.submitSql("select * from ").delay(2.millis).fork
      f4 <- executor.submitSql("show tables").delay(3.millis).fork
      f5 <- executor.submitSql("show catalogs").delay(4.millis).fork
      _  <- f1.join.ignore
      _  <- f2.join.ignore
      _  <- f3.join.ignore
      _  <- f4.join.ignore
      _  <- f5.join.ignore
      _  <- printLine("====================  let me see ==================== ")
      _  <- executor.listHandleFrame.debugPretty
    } yield ()

  }
