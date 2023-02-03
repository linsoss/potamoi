package potamoi.akka

import akka.actor.typed.scaladsl.ActorContext
import potamoi.common.ZIOExtension.zioRunUnsafe
import potamoi.logger.{LogConf, PotaLogger}
import potamoi.logger.PotaLogger.akkaSourceMdc
import potamoi.zios.{asLayer, zioRunToFuture}
import zio.{CancelableFuture, IO, UIO, ZIO, ZIOAspect}
import zio.ZIOAspect.annotated

/**
 * ZIO effect extension on actor
 */
object ActorZIOExtension:

  extension [E <: Throwable, A](zio: IO[E, A])(using ctx: ActorContext[_], logConf: LogConf) {

    inline def runAsync: CancelableFuture[A] = zioRunToFuture {
      zio.provide(logConf.asLayer, PotaLogger.live) @@ annotated(akkaSourceMdc -> ctx.self.path.toString)
    }

    inline def runSync: Either[E, A] = zioRunUnsafe {
      zio.either
        .provide(logConf.asLayer, PotaLogger.live) @@ annotated(akkaSourceMdc -> ctx.self.path.toString)
    }

    inline def runSyncUnion: E | A = runSync match
      case Left(e)  => e
      case Right(a) => a
  }

  extension [A](zio: UIO[A])(using ctx: ActorContext[_], logConf: LogConf) {

    inline def runPureAsync: CancelableFuture[A] = zioRunToFuture {
      zio.provide(logConf.asLayer, PotaLogger.live) @@ annotated(akkaSourceMdc -> ctx.self.path.toString)
    }

    inline def runPureSync: A = zioRunUnsafe {
      zio.provide(logConf.asLayer, PotaLogger.live) @@ annotated(akkaSourceMdc -> ctx.self.path.toString)
    }
  }
