package potamoi.flink.interact

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.Ignore
import potamoi.{BaseConf, HoconConfig, NodeRoles}
import potamoi.akka.{ActorCradle, AkkaConf}
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
import zio.{durationInt, IO, Schedule, Scope, ZIO, ZIOAppDefault, ZLayer}
import zio.ZIO.logInfo

/**
 * Need to launch standalone remote [[potamoi.TestFlinkInterpreterAppV116]] app.
 */
// @Ignore
class FlinkSqlInteractorTest extends AnyWordSpec:

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
        AkkaConf.localCluster(3300, List(3300, 3316), List(NodeRoles.flinkService)).project(_.copy(defaultAskTimeout = 20.seconds)),
        ActorCradle.live,
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

  "session management" in testing { interact =>
    for {
      _         <- interact.manager.listRemoteInterpreter(V116).repeatUntil(_.nonEmpty).debugPrettyWithTag("list remote interpreter").debug
      sessionId <- interact.manager.create(InteractSessionDef.local(), V116).debugPrettyWithTag("create session").debug
      _         <- interact.manager.session.list.runCollect.debugPrettyWithTag("list session")
      _         <- interact.manager.close(sessionId)
      _         <- interact.manager.session.list.runCollect.debugPrettyWithTag("list session")
    } yield ()
  }
