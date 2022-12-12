package potamoi.common

import sttp.client3.{Response, ResponseException, SttpBackend}
import sttp.client3.httpclient.zio.HttpClientZioBackend
import sttp.model.StatusCode
import zio.ZIO
import zio.*

import scala.annotation.targetName

/**
 * Sttp client extension.
 */
object SttpExtension {

  case class HttpRequestNonPass(message: String) extends Exception(message)
  case class ResolveBodyErr(cause: Throwable)    extends Exception(cause)

  /**
   * Using Sttp backend to create http request effect and then release the request
   * backend resource automatically.
   */
  def usingSttp[A](request: SttpBackend[Task, Any] => IO[Throwable, A]): IO[Throwable, A] =
    ZIO.scoped {
      HttpClientZioBackend.scoped().flatMap(backend => request(backend))
    }

  /**
   * Similar to [[usingSttp]] but narrow side effect channel type.
   */
  def usingTypedSttp[E, A](request: SttpBackend[Task, Any] => IO[E, A])(implicit narrowErr: Throwable => E): IO[E, A] =
    ZIO.scoped {
      HttpClientZioBackend
        .scoped()
        .mapError(narrowErr)
        .flatMap(backend => request(backend))
    }

  /**
   * Get response body from Response and narrow error type.
   */
  implicit class ResponseHandler[A](requestIO: Task[Response[Either[String, A]]]) {
    def narrowBody[E](implicit narrowErr: Throwable => E): IO[E, A] =
      requestIO
        .mapError(narrowErr)
        .flatMap(rsp => ZIO.fromEither(rsp.body).mapError(e => narrowErr(HttpRequestNonPass(e))))

    def narrowBodyT[E](notFound: => E)(implicit narrowErr: Throwable => E): IO[E, A] =
      requestIO
        .mapError(narrowErr)
        .flatMap { rsp =>
          rsp.code match {
            case StatusCode.NotFound => ZIO.fail(notFound)
            case _                   => ZIO.fromEither(rsp.body).mapError(e => narrowErr(HttpRequestNonPass(e)))
          }
        }
  }

  /**
   * Get response body from Response and narrow error type.
   */
  implicit class ResponseHandler2[A](requestIO: Task[Response[Either[ResponseException[String, String], A]]]) {
    def narrowBody[E](implicit narrowErr: Throwable => E): IO[E, A] =
      requestIO
        .mapError(narrowErr)
        .flatMap(rsp => ZIO.fromEither(rsp.body).mapError(e => narrowErr(e)))

    def narrowBodyT[E](notFound: => E)(implicit narrowErr: Throwable => E): IO[E, A] =
      requestIO
        .mapError(narrowErr)
        .flatMap { rsp =>
          rsp.code match {
            case StatusCode.NotFound => ZIO.fail(notFound)
            case _                   => ZIO.fromEither(rsp.body).mapError(e => narrowErr(e))
          }
        }
  }

  /**
   * Handle string response body and narrow error type.
   */
  extension [E, A](requestIO: IO[E, String]) {
    def attemptBody(f: String => A)(implicit narrowErr: Throwable => E): IO[E, A] =
      requestIO.flatMap { body =>
        ZIO
          .attempt(f(body))
          .mapError(e => narrowErr(ResolveBodyErr(e)))
      }
  }

  /**
   * Collapse the Either left in the RequestT to the ZIO error channel.
   */
  implicit class RequestBodyIOWrapper[A](requestIO: IO[Throwable, Either[String, A]]) {
    def narrowEither: IO[Throwable, A] = requestIO.flatMap {
      case Left(err)  => ZIO.fail(HttpRequestNonPass(err))
      case Right(rsp) => ZIO.succeed(rsp)
    }
  }

  implicit class RequestDeserializationBodyIOWrapper[DeE <: Throwable, A](requestIO: IO[Throwable, Either[DeE, A]]) {
    def narrowEither: IO[Throwable, A] = requestIO.flatMap {
      case Left(err)  => ZIO.fail(err)
      case Right(rsp) => ZIO.succeed(rsp)
    }
  }

}
