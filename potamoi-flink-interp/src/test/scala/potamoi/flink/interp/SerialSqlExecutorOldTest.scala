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
import zio.{IO, Scope, Task, ZIO}
import zio.Console.printLine
import zio.ZIO.{logErrorCause, logInfo, sleep}

//@Ignore
class SerialSqlExecutorOldTest extends AnyWordSpec:

  def testing[E, A](sessDef: SessionDef)(f: SerialSqlExecutor => ZIO[Scope, E, A]) =
    (for {
      fs       <- ZIO.service[RemoteFsOperator]
      executor <- ZIO.succeed(SerialSqlExecutorImpl("23333", sessDef, fs))
      _        <- executor.stop
      rs       <- f(executor)
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
      desc              <- executor.submitSql("select * from Orders").debugPretty.map { case r: QuerySqlRs => r.handleId -> r.dataStream }
      _                 <- logInfo("receive result stream")
      (handleId, stream) = desc
      _                 <- stream.foreach(row => printLine(row.show))
      _                 <- logInfo("retrieve page")
      _                 <- executor.retrieveResultPage(handleId, 1, 20).debugPretty
    } yield ()
  }

//  "execute sql normally on remote" in testing { executor =>
//    for {
//      _                 <- executor.initEnv(SessionDef.remote(execMode = Streaming, endpoint = "10.233.62.91" -> 8081))
//      _                 <- executor.executeSql(dataGenTableSql).debugPretty
//      _                 <- executor.executeSql("explain select * from Orders").debugPretty
//      _                 <- executor.executeSql("show catalogs").debugPretty
//      _                 <- executor.executeSql("show tables").debugPretty
//      desc              <- executor.executeSql("select * from Orders").debugPretty.map { case r: QuerySqlRs => r.handleId -> r.dataStream }
//      _                 <- logInfo("receive result stream")
//      (handleId, stream) = desc
//      _                 <- stream.foreach(row => printLine(row.show))
//      _                 <- logInfo("retrieve page")
//      _                 <- executor.retrieveResultPage(handleId, 1, 20).debugPretty
//      _                 <- executor.closeEnv
//    } yield ()
//  }
//
//  "complete sql" in testing { executor =>
//    for {
//      _ <- executor.initEnv(SessionDef(execType = Local))
//      _ <- executor.executeSql(dataGenTableSql).debugPretty
//      _ <- logInfo("hint 1") *> executor.completeSql("select * from ").debugPretty
//      _ <- logInfo("hint 2") *> executor.completeSql("select *  Orders", 8).debugPretty
//      _ <- executor.closeEnv
//    } yield ()
//  }
//
//  "execute sql with extra jars on local" in testing { executor =>
//    for {
//      _      <- executor.initEnv(
//                  SessionDef.local(
//                    execMode = Streaming,
//                    jars = List("pota://flink-faker-0.5.1.jar"),
//                    resultStore = ResultStoreConf(20, DropTail)
//                  ))
//      _      <- executor.executeSql(dataFakerTableSql).debugPretty
//      stream <- executor.executeSql("select * from Heros").debugPretty.map { case r: QuerySqlRs => r.dataStream }
//      _      <- stream.foreach(row => printLine(row.show))
//      _      <- executor.closeEnv
//    } yield ()
//  }
//
//  "execute sql with extra jars on remote" in testing { executor =>
//    for {
//      _      <- executor.initEnv(
//                  SessionDef.remote(
//                    endpoint = "10.233.62.91" -> 8081,
//                    execMode = Streaming,
//                    localJars = List("pota://flink-faker-0.5.1.jar"),
//                    clusterJars = List("pota://flink-faker-0.5.1.jar"),
//                    resultStore = ResultStoreConf(20, DropTail)
//                  ))
//      _      <- executor.executeSql(dataFakerTableSql).debugPretty
//      stream <- executor.executeSql("select * from Heros").debugPretty.map { case r: QuerySqlRs => r.dataStream }
//      _      <- stream.foreach(row => printLine(row.show))
//      _      <- executor.closeEnv
//    } yield ()
//  }
//
//  "execute add jar sql on local" in testing { executor =>
//    for {
//      _ <- executor.initEnv(SessionDef.local())
//      _ <- executor.executeSql("add jar 'pota://flink-faker-0.5.1.jar';").debugPretty
//      _ <- executor.executeSql("show jars").debugPretty
//      _ <- executor.executeSql(dataFakerTableSql).debugPretty
//      _ <- executor
//             .executeSql("select * from Heros;")
//             .debugPretty
//             .map { case r: QuerySqlRs => r.dataStream }
//             .flatMap { stream =>
//               stream.foreach(row => printLine(row.show))
//             }
//      _ <- executor.closeEnv
//    } yield ()
//  }
//
//  "execute add jar sql on remote" in testing { executor =>
//    for {
//      _ <- executor.initEnv(SessionDef.remote(endpoint = "10.233.62.91" -> 8081))
//      _ <- executor.executeSql("add jar 'pota://flink-faker-0.5.1.jar';").debugPretty
//      _ <- executor.executeSql("show jars").debugPretty
//      _ <- executor.executeSql(dataFakerTableSql).debugPretty
//      _ <- executor
//             .executeSql("select * from Heros;")
//             .debugPretty
//             .map { case r: QuerySqlRs => r.dataStream }
//             .flatMap { stream =>
//               stream.foreach(row => printLine(row.show))
//             }
//      _ <- executor.closeEnv
//    } yield ()
//  }
//
//  "view handle frame" in testing { executor =>
//    ???
//  }
//
//  "cancel current handle" in testing { executor =>
//    ???
//  }
