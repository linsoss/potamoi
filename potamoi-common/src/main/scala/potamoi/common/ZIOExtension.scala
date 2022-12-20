package potamoi.common

import zio.{Exit, *}
import potamoi.syntax.toPrettyString
import potamoi.errs.recurse

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
  }

  extension [R, E, A](zio: ZIO[R, E, A]) {
    inline def debugPretty: ZIO[R, E, A] = zio
      .tap(value => ZIO.succeed(println(toPrettyString(value))))
      .tapErrorCause {
        case cause: Cause[Throwable] => ZIO.succeed(println(s"<FAIL> ${cause.recurse.prettyPrint}"))
        case cause                   => ZIO.succeed(println(s"<FAIL> ${cause.prettyPrint}"))
      }
  }

  extension [E, A](zio: ZIO[Scope, E, A]) {
    def endScoped(): IO[E, A] = ZIO.scoped(zio)
  }

  /**
   * Close resource zio.
   */
  def close(resource: AutoCloseable): UIO[Unit] = ZIO.succeed(resource.close())

  /**
   * [[scala.util.Using]] style syntax for ZIO.
   */
  def usingAttempt[RS <: AutoCloseable](code: => RS): ZIO[Scope, Throwable, RS] = ZIO.acquireRelease(ZIO.attempt(code))(close)

  /**
   * [[scala.util.Using]] style syntax for ZIO.
   */
  def usingAttemptBlocking[RS <: AutoCloseable](code: => RS): ZIO[Scope, Throwable, RS] =
    ZIO.acquireRelease(ZIO.attemptBlockingInterrupt(code))(close)

  /**
   * Convert product to a [[ZLayer]].
   */
  extension [A <: Product: Tag](product: A) def asLayer: ULayer[A] = ZLayer.succeed(product)
}

final case class FutureErr[T](reason: T) extends Err(toPrettyString(reason))
