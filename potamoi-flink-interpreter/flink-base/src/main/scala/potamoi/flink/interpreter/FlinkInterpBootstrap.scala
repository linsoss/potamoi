package potamoi.flink.interpreter

import com.devsisters.shardcake.{EntityType, Sharding}
import potamoi.flink.FlinkMajorVer
import potamoi.fs.refactor.{FsBackendConf, RemoteFsOperator}
import potamoi.logger.{LogConf, PotaLogger}
import potamoi.sharding.{ShardingConf, Shardings}
import potamoi.sharding.store.ShardRedisStoreConf
import potamoi.BaseConf
import potamoi.common.HoconConfig
import potamoi.flink.protocol.{FlinkInterpEntity, FlinkInterpProto}
import zio.{Scope, ZIO, ZIOAppDefault, ZLayer}
import zio.ZIO.logInfo
import zio.http.{Server, *}
import zio.http.model.Method

import java.net.InetSocketAddress

/**
 * Flink interpreter app bootstrap.
 */
abstract class FlinkInterpBootstrap(flinkVer: FlinkMajorVer) extends ZIOAppDefault:

  override val bootstrap = LogConf.live >>> PotaLogger.live
  val shardEntity        = FlinkInterpEntity.adapters(flinkVer)

  def active: ZIO[Sharding with Scope with FlinkInterpConf with FlinkSqlInterpreter, Throwable, Unit] =
    for {
      _           <- logInfo(s"Flink interpreter launching, flink-version: ${flinkVer.value}")
      interpreter <- ZIO.service[FlinkSqlInterpreter]
      interpConf  <- ZIO.service[FlinkInterpConf]
      // register sharding entity
      _           <- interpreter.registerEntities
      _           <- Sharding.registerScoped
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
    FsBackendConf.live,
    ShardingConf.live,
    ShardRedisStoreConf.live,
    FlinkInterpConf.live,
    Shardings.live,
    RemoteFsOperator.live,
    FlinkSqlInterpreter.live(shardEntity),
    Scope.default
  )
