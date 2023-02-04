package potamoi.flink.interact

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.Ignore
import potamoi.{BaseConf, HoconConfig, NodeRoles}
import potamoi.akka.{AkkaConf, AkkaMatrix}
import potamoi.debugs.*
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.FlinkConf
import potamoi.flink.model.interact.*
import potamoi.flink.storage.FlinkDataStorage
import potamoi.flink.FlinkMajorVer.*
import potamoi.flink.interpreter.{FlinkInterpConf, FlinkInterpreterPier}
import potamoi.fs.refactor.backend.S3FsMirrorBackend
import potamoi.fs.S3FsBackendConfDev
import potamoi.kubernetes.{K8sConf, K8sOperator}
import potamoi.logger.{LogConf, LogsLevel, PotaLogger}
import potamoi.times.given_Conversion_ZIODuration_ScalaDuration
import potamoi.zios.*
import potamoi.NodeRoles.{flinkInterpreter, flinkService}
import potamoi.flink.model.Fcid
import potamoi.flink.model.FlinkRuntimeMode.Streaming
import potamoi.flink.model.interact.HandleStatus.{Cancel, Finish, Run}
import potamoi.flink.model.interact.ResultDropStrategy.{DropHead, DropTail}
import potamoi.syntax.toPrettyStr
import zio.{durationInt, IO, Schedule, Scope, ZIO, ZIOAppDefault, ZLayer}
import zio.Console.printLine
import zio.ZIO.logInfo

object FlinkSqlInteractorTest:

  // Need to launch standalone remote app [[potamoi.TestFlinkInterpreterAppV116]] when switch on.
  val testWithRemoteInterpreter = true

  val remoteFlinkFcid = Fcid("session-t16", "fdev")

  val akkaConf = {
    if !testWithRemoteInterpreter then AkkaConf.local(List(flinkService, flinkInterpreter(V116.seq)))
    else AkkaConf.localCluster(3300, List(3300, 3316), List(flinkService))
  }.project(_.copy(defaultAskTimeout = 15.seconds))

  def testing[E, A](effect: FlinkSqlInteractor => IO[E, A]): Unit = {
    ZIO
      .scoped {
        for {
          _ <- FlinkDataStorage.active
          _ <- FlinkObserver.active
          _ <- FlinkSqlInteractor.active
          _ <- ZIO
                 .service[FlinkSqlInteractor]
                 .flatMap { interactor =>
                   ZIO.logInfo("Wait for flink interpreter...") *>
                   interactor.manager.listRemoteInterpreter(V116).repeatUntil(_.nonEmpty) *>
                   ZIO.logInfo("Let's testing!") *>
                   effect(interactor)
                 }
        } yield ()
      }
      .provide(
        HoconConfig.empty,
        LogConf.default,
        BaseConf.test,
        S3FsBackendConfDev.asLayer,
        S3FsMirrorBackend.live,
        K8sConf.default,
        K8sOperator.live,
        akkaConf,
        AkkaMatrix.live,
        FlinkConf.test,
        FlinkDataStorage.test,
        FlinkObserver.live,
        FlinkSqlInteractor.live,
        Scope.default
      )
      .provideLayer(PotaLogger.default)
      .run
      .exitCode
  }

  extension (conn: SessionConnection)
    def executeSql(sql: String) = {
      for
        handleId <- conn.submitSqlAsync(sql).debugPrettyWithTag("handle-id")
        _        <- conn
                      .subscribeHandleFrame(handleId)
                      .hybridChanging
                      .mapZIO {
                        case (frame, None)         => printLine(frame.toPrettyStr)
                        case (frame, Some(stream)) => printLine(frame.toPrettyStr) *> stream.tap(row => printLine(row.show)).runDrain
                      }
                      .runDrain
      yield handleId
    }

import potamoi.flink.interact.FlinkSqlInteractorTest.*

// @Ignore
class FlinkSqlInteractorTest extends AnyWordSpec:

  val dataGenTableSql =
    """CREATE TABLE Orders (
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

  val dataGenEndlessTableSql =
    """CREATE TABLE Orders (
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

  val dataFakerTableSql =
    """CREATE TABLE Heros (
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

  "session management" in testing { interact =>
    for {
      _         <- interact.manager.listRemoteInterpreter(V116).repeatUntil(_.nonEmpty).debugPrettyWithTag("list remote interpreter").debug
      sessionId <- interact.manager.create(InteractSessionDef.local(), V116).debugPrettyWithTag("create session").debug
      _         <- interact.manager.session.list.runCollect.debugPrettyWithTag("list session")
      _         <- interact.manager.close(sessionId)
      _         <- interact.manager.session.list.runCollect.debugPrettyWithTag("list session")
      _         <- ZIO.never
    } yield ()
  }

  "execute sql simply and subscribe handle-frame" in testing { intr =>
    for {
      sessionId <- intr.manager.create(InteractSessionDef.local(), V116).debugPrettyWithTag("session-id")
      conn      <- intr.attach(sessionId)
      handleId  <- conn.submitSqlAsync(dataGenTableSql).debugPrettyWithTag("handle-id")
      _         <- conn.subscribeHandleFrame(handleId).ending.debugPrettyWithTag("handle-frame")
    } yield ()
  }

  "execute sql simply and poll handle-frame" in testing { intr =>
    for {
      sessionId <- intr.manager.create(InteractSessionDef.local(), V116).debugPrettyWithTag("session-id")
      conn      <- intr.attach(sessionId)
      handleId  <- conn.submitSqlAsync(dataGenTableSql).debugPrettyWithTag("handle-id")
      f1        <- conn
                     .getHandleFrame(handleId)
                     .debugPrettyWithTag("get handle frame")
                     .repeatWhileWithSpaced(e => HandleStatuses.isEnd(e.status), 500.millis)
                     .fork
      f2        <- conn
                     .getHandleStatus(handleId)
                     .debugPrettyWithTag("get handle status")
                     .repeatWhileWithSpaced(e => HandleStatuses.isEnd(e.status), 500.millis)
                     .fork

      _ <- f1.join
      _ <- f2.join
    } yield ()
  }

  "execute sql normally on local" in testing { intr =>
    for
      sessionId <- intr.manager.create(InteractSessionDef.local(), V116)
      conn      <- intr.attach(sessionId)
      _         <- conn.executeSql(dataGenTableSql)
      _         <- conn.executeSql("explain select * from Orders")
      _         <- conn.executeSql("show catalogs")
      _         <- conn.executeSql("show tables")
      handleId  <- conn.executeSql("select * from Orders")
      _         <- printLine("retrieve rs via page:")
      _         <- conn.retrieveResultPage(handleId, 1, 20).debugPretty
      _         <- printLine("retrieve rs via offset:")
      _         <- conn.retrieveResultOffset(handleId, -1, 10).debugPretty
      _         <- printLine("list handleId")
      _         <- conn.listHandleId.debugPretty
      _         <- printLine("list handle status")
      _         <- conn.listHandleStatus.debugPretty
      _         <- printLine("list handle frame")
      _         <- conn.listHandleFrame.debugPretty
      _         <- intr.manager.close(sessionId)
    yield ()
  }

  "execute sql normally on remote" in testing { intr =>
    for
      sessionId <- intr.manager.create(InteractSessionDef.remote(remoteFlinkFcid), V116)
      conn      <- intr.attach(sessionId)
      _         <- conn.executeSql(dataGenTableSql)
      _         <- conn.executeSql("explain select * from Orders")
      _         <- conn.executeSql("show catalogs")
      _         <- conn.executeSql("show tables")
      _         <- conn.executeSql("select * from Orders")
      _         <- intr.manager.close(sessionId)
    yield ()
  }

  "complete sql" in testing { intr =>
    for
      sessionId <- intr.manager.create(InteractSessionDef.remote(remoteFlinkFcid), V116)
      conn      <- intr.attach(sessionId)
      _         <- conn.executeSql(dataGenTableSql)
      _         <- conn.completeSql("select * from ").debugPrettyWithTag("hint 1")
      _         <- conn.completeSql("select *  Orders", 8).debugPrettyWithTag("hint 2")
      _         <- intr.manager.close(sessionId)
    yield ()
  }

  "execute sql with extra jars on local" in testing { intr =>
    for
      sessionId <- intr.manager create (
                     InteractSessionDef.local(
                       execMode = Streaming,
                       jars = List("pota://flink-faker-0.5.1.jar"),
                       resultStore = ResultStoreConf(20, DropTail)
                     ),
                     V116
                   )
      conn      <- intr attach sessionId
      _         <- conn executeSql dataFakerTableSql
      _         <- conn executeSql "select * from Heros"
      _         <- intr.manager close sessionId
    yield ()
  }

  "execute sql with extra jars on remote" in testing { intr =>
    for
      sessionId <- intr.manager.create(
                     InteractSessionDef.remote(
                       execMode = Streaming,
                       remoteCluster = remoteFlinkFcid,
                       localJars = List("pota://flink-faker-0.5.1.jar"),
                       clusterJars = List("pota://flink-faker-0.5.1.jar"),
                       resultStore = ResultStoreConf(20, DropTail)
                     ),
                     V116
                   )
      conn      <- intr attach sessionId
      _         <- conn executeSql dataFakerTableSql
      _         <- conn executeSql "select * from Heros"
      _         <- intr.manager close sessionId
    yield ()
  }

  "execute add jar sql on local" in testing { intr =>
    for
      sessionId <- intr.manager.create(InteractSessionDef.local(resultStore = ResultStoreConf(20, DropTail)), V116)
      conn      <- intr attach sessionId
      _         <- conn executeSql "add jar 'pota://flink-faker-0.5.1.jar'"
      _         <- conn executeSql "show jars"
      _         <- conn executeSql dataFakerTableSql
      _         <- conn.executeSql("select * from Heros")
      _         <- intr.manager.close(sessionId)
    yield ()
  }

  "execute add jar sql on remote" in testing { intr =>
    for
      sessionId <- intr.manager.create(
                     InteractSessionDef.remote(
                       remoteCluster = remoteFlinkFcid,
                       resultStore = ResultStoreConf(20, DropTail)
                     ),
                     V116
                   )
      conn      <- intr attach sessionId
      _         <- conn executeSql "add jar 'pota://flink-faker-0.5.1.jar'"
      _         <- conn executeSql "show jars"
      _         <- conn executeSql dataFakerTableSql
      _         <- conn.executeSql("select * from Heros")
      _         <- intr.manager.close(sessionId)
    yield ()
  }

  "execute set sql on local" in testing { intr =>
    for
      sessionId <- intr.manager.create(InteractSessionDef.local(resultStore = ResultStoreConf(20, DropTail)), V116)
      conn      <- intr.attach(sessionId)
      _         <- conn.executeSql(dataFakerTableSql)
      _         <- conn.executeSql("show tables;")
      _         <- conn.executeSql("set execution.parallelism=3;")
      _         <- conn.executeSql("set;")
      _         <- intr.manager.close(sessionId)
    yield ()
  }

  "cancel handle on local" in testing { intr =>
    for
      sessionId <- intr.manager.create(InteractSessionDef.local(), V116)
      conn      <- intr.attach(sessionId)
      _         <- conn.executeSql(dataGenEndlessTableSql)
      _         <- conn.executeSql("select * from Orders").fork
      _         <- printLine("cancel current handle frames!")
      _         <- intr.manager.cancel(sessionId).delay(15.seconds)
      _         <- conn.listHandleFrame.debugPrettyWithTag("list handle frame").delay(3.seconds)
      _         <- ZIO.never
    yield ()
  }

  "cancel handle on remote" in testing { intr =>
    for
      sessionId <- intr.manager.create(InteractSessionDef.remote(remoteFlinkFcid), V116)
      conn      <- intr.attach(sessionId)
      _         <- conn.executeSql(dataGenEndlessTableSql)
      _         <- conn.executeSql("select * from Orders").fork
      _         <- printLine("cancel current handle frames!")
      _         <- intr.manager.cancel(sessionId).delay(15.seconds)
      _         <- conn.listHandleFrame.debugPrettyWithTag("list handle frame").delay(3.seconds)
      _         <- ZIO.never
    yield ()
  }

  "submit sql script" in testing { intr =>

    val script: String =
      """CREATE TABLE Orders (
        |    order_number BIGINT,
        |    price        DECIMAL(32,2),
        |    buyer        ROW<first_name STRING, last_name STRING>,
        |    mset        MULTISET<STRING>,
        |    order_time   TIMESTAMP(3)
        |) WITH (
        |  'connector' = 'datagen',
        |  'number-of-rows' = '10'
        |);
        |SHOW TABLES;
        |select * from Orders;
        |""".stripMargin

    for
      sessionId <- intr.manager.create(InteractSessionDef.local(), V116)
      conn      <- intr.attach(sessionId)
      signs     <- conn.submitSqlScriptAsync(script).debugPrettyWithTag("submit sql")
      _         <- conn
                     .subscribeScriptResultFrame(signs.map(_.handleId))
                     .hybridChanging
                     .mapZIO {
                       case (frame, None)         => printLine(frame.toPrettyStr)
                       case (frame, Some(stream)) => printLine(frame.toPrettyStr) *> stream.tap(row => printLine(row.show)).runDrain
                     }
                     .runDrain
      _         <- intr.manager.close(sessionId)
    yield ()
  }
