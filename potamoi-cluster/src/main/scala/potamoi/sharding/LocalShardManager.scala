package potamoi.sharding

import com.devsisters.shardcake
import potamoi.logger.PotaLogger
import potamoi.zios.asLayer
import zio.*
import ZIO.logInfo
import com.devsisters.shardcake.Server
import zio.http.Client
import zio.http.model.Status

/**
 * Shardcake manager application for local standalone debugging.
 */
object LocalShardManager:

  extension [R, E, A](zio: ZIO[R, E, A]) def withLocalShardManager: ZIO[R, E, A] = wrap(zio)

  inline def wrap[R, E, A](zio: ZIO[R, E, A]): ZIO[R, E, A] = {
    for {
      _           <- logInfo("Launch shardcake manager server...")
      serverFiber <- runLocalDebuggingServer.fork
      _           <- waitReady.orDie
      _           <- logInfo("Shardcake manager server is ready.")
      result      <- zio
      _           <- logInfo("Interrupt sharkcake manager fiber.")
      _           <- serverFiber.interrupt.ignore
    } yield result
  }

  private def runLocalDebuggingServer = Server.run
    .provide(ShardManagerConf.test.asLayer, ShardManagers.test)
    .provideLayer(PotaLogger.default)

  private def waitReady: IO[Throwable, Unit] = {
    val ef = for {
      mgrConf <- ZIO.service[ShardManagerConf]
      _ <- Client
        .request(s"http://127.0.0.1:${mgrConf.port}/health")
        .map(_.status == Status.Ok)
        .catchAll(_ => ZIO.succeed(false))
        .repeatUntil(e => e)
    } yield ()
    ef.provide(ShardManagerConf.test.asLayer, Client.default, Scope.default)
  }
