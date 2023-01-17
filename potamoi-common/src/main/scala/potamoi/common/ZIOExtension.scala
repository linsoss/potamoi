package potamoi.common

import potamoi.syntax.toPrettyString
import potamoi.PotaErr
import zio.{Exit, *}
import zio.stream.ZStream

import scala.reflect.ClassTag

/**
 * ZIO syntax extension.
 */
object ZIOExtension {

  extension [E, A](zio: IO[E, A]) {
    inline def run: Exit[E, A] = zioRun(zio)
    inline def runUnsafe: A    = Unsafe.unsafe { implicit u => Runtime.default.unsafe.run(zio).getOrThrowFiberFailure() }

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

    inline def repeatWhileWithSpaced(f: A => Boolean, spaced: Duration): ZIO[R, E, A] =
      zio.repeat(Schedule.recurWhile[A](f) && Schedule.spaced(spaced)).map(_._1)
  }

  extension [A](uio: UIO[A]) {
    inline def runNow: A = Unsafe.unsafe { implicit u =>
      Runtime.default.unsafe
        .run(uio)
        .getOrThrowFiberFailure()
    }
  }

  /**
   * Unsafe running ZIO.
   */
  inline def zioRun[E, A](zio: IO[E, A]): Exit[E, A] =
    Unsafe.unsafe(implicit u => Runtime.default.unsafe.run(zio))

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
