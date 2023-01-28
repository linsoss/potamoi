package potamoi.flink.interpreter

import com.devsisters.shardcake.{EntityType, Sharding}
import potamoi.flink.{FlinkConf, FlinkMajorVer}
import potamoi.fs.refactor.{FsBackendConf, RemoteFsOperator}
import potamoi.logger.{LogConf, PotaLogger}
import potamoi.sharding.{ShardingConf, Shardings}
import potamoi.sharding.store.ShardRedisStoreConf
import potamoi.{BaseConf, HoconConfig}
import potamoi.flink.model.interact.InterpreterPod
import potamoi.flink.protocol.{FlinkInterpEntity, FlinkInterpProto, InternalRpcEntity}
import potamoi.rpc.Rpc
import zio.{Scope, ZIO, ZIOAppDefault, ZLayer}
import zio.ZIO.logInfo
import zio.http.*
import zio.http.model.Method
import potamoi.flink.protocol.InternalRpcProto.*

import java.net.InetSocketAddress

/**
 * Flink interpreter app bootstrap.
 */
abstract class FlinkInterpBootstrap(flinkVer: FlinkMajorVer) extends ZIOAppDefault:

  override val bootstrap = LogConf.live >>> PotaLogger.live
  val shardEntity        = FlinkInterpEntity.adapters(flinkVer)

  def active: ZIO[Sharding with Scope with ShardingConf with FlinkInterpConf with FlinkSqlInterpreter, Throwable, Unit] =
    for {
      _           <- logInfo(s"Flink interpreter launching, flink-version: ${flinkVer.value}")
      interpreter <- ZIO.service[FlinkSqlInterpreter]
      interpConf  <- ZIO.service[FlinkInterpConf]
      shardConf   <- ZIO.service[ShardingConf]
      // register sharding entity
      _           <- interpreter.registerEntities
      _           <- Sharding.registerScoped
      // report register/unregister events via internal rpc
      internalRpc <- Rpc(InternalRpcEntity)
      _           <- internalRpc.ask(RegisterFlinkInterpreter(InterpreterPod(flinkVer, shardConf.selfHost, shardConf.selfPort), _)).retryN(3)
      _           <- Sharding.register.withFinalizer { _ =>
                       internalRpc.tell(UnregisterFlinkInterpreter(flinkVer, shardConf.selfHost, shardConf.selfPort))
                     }
      // launch health http api
      _           <- Server
                       .serve(healthApi)
                       .provide(
                         ServerConfig.live ++ ServerConfig.live.project(_.binding(InetSocketAddress(interpConf.httpPort))),
                         Server.live
                       )
      _           <- ZIO.never
    } yield ()

  lazy val healthApi = Http.collectHttp[Request] {
    case Method.GET -> !! / "health"  => Http.ok
    case Method.GET -> !! / "version" => Http.text(flinkVer.value)
  }

  val run = active.provide(
    HoconConfig.live,
    BaseConf.live,
    FlinkConf.live,
    FsBackendConf.live,
    ShardingConf.live,
    ShardRedisStoreConf.live,
    FlinkInterpConf.live,
    Shardings.live,
    RemoteFsOperator.live,
    FlinkSqlInterpreter.live(shardEntity),
    Scope.default
  )
