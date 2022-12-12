package potamoi.common

import zio.{IO, ZIO}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

/**
 * Extension for Scala Future.
 */
object FutureExtension {

  inline def sleep(duration: Duration): Unit = Thread.sleep(duration.toMillis)

  extension[A](future: Future[A]) {

    /**
     * Convert to ZIO.
     */
    def asZIO: IO[Throwable, A] = ZIO.fromFuture(implicit ec => future)

    /**
     * Blocking Future and wait for result.
     */
    def blocking(atMost: Duration = Duration.Inf): A = Await.result(future, atMost)

    /**
     * Blocking Future and wait for result with Try.
     */
    def safeBlocking(atMost: Duration = Duration.Inf): Either[Throwable, A] = Try(Await.result(future, atMost)).toEither
  }

}
