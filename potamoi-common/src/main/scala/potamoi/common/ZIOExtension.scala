package potamoi.common

import akka.actor.typed.scaladsl.ActorContext
import potamoi.syntax.toPrettyString
import potamoi.PotaErr
import potamoi.logger.{LogConf, PotaLogger}
import potamoi.logger.PotaLogger.akkaSourceMdc
import zio.{Exit, *}
import zio.stream.ZStream

import scala.reflect.ClassTag

/**
 * ZIO syntax extension.
 */
object ZIOExtension {

  extension [E, A](zio: IO[E, A]) {
    inline def run: Exit[E, A] = zioRun(zio)
    inline def runUnsafe: A    = zioRunUnsafe(zio)

    inline def distPollStream(spaced: Duration): ZStream[Any, E, A] =
      for {
        ref    <- ZStream.fromZIO(Ref.make[Option[A]](None))
        stream <- ZStream
                    .fromZIO(zio)
                    .repeat(Schedule.spaced(spaced))
                    .filterZIO { a =>
                      for {
                        pre <- ref.get
                        pass = !pre.contains(a)
                        _   <- ref.set(Some(a))
                      } yield pass
                    }
      } yield stream
  }

  extension [R, E, A](zio: ZIO[R, E, A]) {
    inline def debugPretty: ZIO[R, E, A] =
      zio
        .tap(value => ZIO.succeed(println(toPrettyString(value))))
        .tapErrorCause { case cause =>
          ZIO.succeed(println(s"<FAIL> ${cause.prettyPrint}"))
        }

    inline def debugPrettyWithTag(tag: String): ZIO[R, E, A] =
      zio
        .tap(value => ZIO.succeed(println(s"$tag: ${toPrettyString(value)}")))
        .tapErrorCause { case cause =>
          ZIO.succeed(println(s"<FAIL> $tag: ${cause.prettyPrint}"))
        }

    inline def repeatWhileWithSpaced(f: A => Boolean, spaced: Duration): ZIO[R, E, A] =
      zio.repeat(Schedule.recurWhile[A](f) && Schedule.spaced(spaced)).map(_._1)
  }

  extension [A](uio: UIO[A]) {
    inline def runNow: A = zioRunUnsafe(uio)
  }

  extension [E, A](zio: IO[E, Option[A]]) {
    inline def someOrUnit[E1 >: E](f: A => IO[E1, Unit]) =
      zio.flatMap {
        case None        => ZIO.unit
        case Some(value) => f(value)
      }

    inline def someOrFailUnion[E1](e: => E1): IO[E | E1, A] =
      zio.flatMap {
        case None        => ZIO.fail(e)
        case Some(value) => ZIO.succeed(value)
      }
  }

  /**
   * Unsafe running ZIO.
   */
  inline def zioRun[E, A](zio: IO[E, A]): Exit[E, A] =
    Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(zio))

  inline def zioRunUnsafe[E, A](zio: IO[E, A]): A =
    Unsafe.unsafe { implicit u => Runtime.default.unsafe.run(zio).getOrThrowFiberFailure() }

  /**
   * Unsafe running ZIO to future.
   */
  inline def zioRunToFuture[E <: Throwable, A](zio: IO[E, A]): CancelableFuture[A] =
    Unsafe.unsafe { implicit u => Runtime.default.unsafe.runToFuture(zio) }

  /**
   * Close resource zio.
   */
  inline def close(resource: AutoCloseable): UIO[Unit] = ZIO.succeed(resource.close())

  /**
   * [[scala.util.Using]] style syntax for ZIO.
   */
  inline def usingAttempt[RS <: AutoCloseable](code: => RS): ZIO[Scope, Throwable, RS] = ZIO.acquireRelease(ZIO.attempt(code))(close(_))

  /**
   * [[scala.util.Using]] style syntax for ZIO.
   */
  inline def usingAttemptBlocking[RS <: AutoCloseable](code: => RS): ZIO[Scope, Throwable, RS] =
    ZIO.acquireRelease(ZIO.attemptBlockingInterrupt(code))(close(_))

  /**
   * Convert product to a [[ZLayer]].
   */
  extension [A <: Product: Tag](product: A) inline def asLayer: ULayer[A] = ZLayer.succeed(product)
}
