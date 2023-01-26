package potamoi.flink.interact

import com.devsisters.shardcake.Sharding
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.Ignore
import potamoi.flink.observer.FlinkObserver
import potamoi.BaseConf
import potamoi.debugs.*
import potamoi.flink.{FlinkConf, InternalRpcService}
import potamoi.flink.model.interact.{InteractSessionDef, InterpreterPod}
import potamoi.flink.storage.FlinkDataStorage
import potamoi.flink.FlinkMajorVer.*
import potamoi.kubernetes.{K8sConf, K8sOperator}
import potamoi.logger.PotaLogger
import potamoi.sharding.{ShardingConf, Shardings}
import potamoi.sharding.LocalShardManager.withLocalShardManager
import potamoi.sharding.store.ShardRedisStoreConf
import potamoi.zios.*
import zio.{durationInt, IO, Schedule, ZIO}
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
          _ <- InternalRpcService.registerEntities
          _ <- Sharding.registerScoped
          _ <- ZIO
                 .service[FlinkSqlInteractor]
                 .flatMap { interactor =>
                   logInfo("Wait for remote flink interpreter v116...") *>
                   interactor.manager.pod.exists(V116).repeatUntil(identity) *>
                   logInfo("Remote flink interpreter ready.") *>
                   effect(interactor)
                 }
                 .tapErrorCause(cause => ZIO.logErrorCause(cause))
        } yield ()
      }
      .provide(
        BaseConf.test,
        FlinkConf.test,
        FlinkDataStorage.test,
        ShardingConf.test,
        Shardings.test,
        K8sConf.default,
        K8sOperator.live,
        FlinkObserver.live,
        InternalRpcService.live,
        FlinkSqlInteractor.live,
      )
      .withLocalShardManager
      .provideLayer(PotaLogger.default)
      .run
      .exitCode
  }

  "session management" in testing { interact =>
    for {
      _         <- interact.manager.pod.list.debugPrettyWithTag("list pods")
      _         <- ZIO.sleep(60.seconds)
      sessionId <- interact.manager.create(InteractSessionDef.local(), V116).debugPrettyWithTag("create session")
//      _         <- interact.manager.session.list.runCollect.debugPrettyWithTag("list session")
//      _         <- interact.manager.close(sessionId)
    } yield ()
  }
