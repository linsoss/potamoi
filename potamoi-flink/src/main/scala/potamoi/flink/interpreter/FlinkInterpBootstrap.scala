package potamoi.flink.interpreter

import potamoi.{BaseConf, HoconConfig}
import potamoi.akka.{AkkaMatrix, AkkaConf}
import potamoi.flink.{FlinkConf, FlinkMajorVer}
import potamoi.fs.refactor.{FsBackendConf, RemoteFsOperator}
import potamoi.logger.{LogConf, PotaLogger}
import zio.{Scope, ZIO, ZIOAppDefault, ZLayer}
import zio.ZIO.logInfo
import zio.http.*
import zio.http.model.Method

import java.net.InetSocketAddress

/**
 * Flink interpreter app bootstrap.
 */
abstract class FlinkInterpBootstrap(flinkVer: FlinkMajorVer) extends ZIOAppDefault:

  override val bootstrap = LogConf.live >>> PotaLogger.live

  def program = for {
    _           <- logInfo(s"Flink interpreter launching, flink-version: ${flinkVer.value}")
    // active akka actor system
    interpConf  <- ZIO.service[FlinkInterpConf]
    _           <- AkkaMatrix.active
    interpreter <- FlinkInterpreterPier.active(flinkVer)
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

  val run = program.provide(
    HoconConfig.live,
    LogConf.live,
    BaseConf.live,
    FsBackendConf.live,
    RemoteFsOperator.live,
    AkkaConf.live,
    AkkaMatrix.live,
    FlinkConf.live,
    FlinkInterpConf.live,
    Scope.default
  )
