package potamoi.flink.interact

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.Ignore
import potamoi.{BaseConf, HoconConfig, NodeRoles}
import potamoi.akka.{AkkaMatrix, AkkaConf}
import potamoi.debugs.*
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.FlinkConf
import potamoi.flink.model.interact.InteractSessionDef
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
import zio.{durationInt, IO, Schedule, Scope, ZIO, ZIOAppDefault, ZLayer}
import zio.ZIO.logInfo

object FlinkSqlInteractorTest:
  // Need to launch standalone remote [[potamoi.TestFlinkInterpreterAppV116]] app.
  val testWithRemote = true

  val akkaConf = {
    if !testWithRemote then AkkaConf.local(List(flinkService, flinkInterpreter(V116.seq)))
    else AkkaConf.localCluster(3300, List(3300, 3316), List(flinkService))
  }.project(_.copy(defaultAskTimeout = 20.seconds))

  def testing[E, A](effect: FlinkSqlInteractor => IO[E, A]): Unit = {
    ZIO
      .scoped {
        for {
          _ <- FlinkDataStorage.active
          _ <- FlinkObserver.active
          _ <- FlinkSqlInteractor.active
          _ <- ZIO
                 .service[FlinkSqlInteractor]
                 .flatMap { interactor => effect(interactor) }
                 .tapErrorCause(cause => ZIO.logErrorCause(cause))
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
      .provideLayer(PotaLogger.layer(level = LogsLevel.Debug))
      .run
      .exitCode
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
    } yield ()
  }
