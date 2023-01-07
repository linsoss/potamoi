package potamoi.common

import potamoi.errs.recurse
import potamoi.syntax.toPrettyString
import zio.{Exit, *}
import zio.stream.ZStream

import scala.reflect.ClassTag

/**
 * ZIO syntax extension.
 */
object ZIOExtension {

  /**
   * Unsafe running ZIO.
   */
  inline def zioRun[E, A](zio: IO[E, A]): Exit[E, A] =
    Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(zio))

  /**
   * Unsafe running ZIO to Future
   */
  inline def zioRunToFuture[E, A](zio: IO[E, A]): CancelableFuture[A] =
    Unsafe.unsafe { implicit u =>
      Runtime.default.unsafe.runToFuture(zio.mapError {
        case e: Throwable => e
        case e            => FutureErr[E](e)
      })
    }

  extension [E, A](zio: IO[E, A]) {
    inline def run: Exit[E, A]                  = zioRun(zio)
    inline def runToFuture: CancelableFuture[A] = zioRunToFuture(zio)

    inline def distPollStream(spaced: Duration): ZStream[Any, E, A] =
      for {
        ref <- ZStream.fromZIO(Ref.make[Option[A]](None))
        stream <- ZStream
          .fromZIO(zio)
          .repeat(Schedule.spaced(spaced))
          .filterZIO { a =>
            for {
              pre <- ref.get
              pass = !pre.contains(a)
              _ <- ref.set(Some(a))
            } yield pass
          }
      } yield stream
  }

  extension [R, E, A](zio: ZIO[R, E, A]) {
    inline def debugPretty: ZIO[R, E, A] = zio
      .tap(value => ZIO.succeed(println(toPrettyString(value))))
      .tapErrorCause {
        case cause: Cause[Throwable] => ZIO.succeed(println(s"<FAIL> ${cause.recurse.prettyPrint}"))
        case cause                   => ZIO.succeed(println(s"<FAIL> ${toPrettyString(cause)}"))
      }

    inline def repeatWhileWithSpaced(f: A => Boolean, spaced: Duration): ZIO[R, E, A] =
      zio.repeat(Schedule.recurWhile[A](f) && Schedule.spaced(spaced)).map(_._1)
  }

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

final case class FutureErr[T](reason: T) extends Err(toPrettyString(reason))
