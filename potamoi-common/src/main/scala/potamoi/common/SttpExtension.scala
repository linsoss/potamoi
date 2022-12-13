package potamoi.common

import sttp.client3.{Response, ResponseException, SttpBackend}
import sttp.client3.httpclient.zio.HttpClientZioBackend
import sttp.model.StatusCode
import zio.ZIO
import zio.*

import scala.annotation.targetName
import scala.util.control.NoStackTrace

/**
 * Sttp client extension.
 */

object SttpExtension {

  trait SttpErr                        extends Throwable
  case class NotPass(cause: Throwable) extends Exception(cause) with SttpErr
  case class NotOk(message: String)    extends Exception(message) with SttpErr
  case class NotOkT(cause: Throwable)  extends Exception(cause) with SttpErr
  case object NotFound                 extends NoStackTrace with SttpErr

  /**
   * Using Sttp backend to create http request effect and then release the request
   * backend resource automatically.
   */
  def usingSttp[A](request: SttpBackend[Task, Any] => IO[Throwable, A]): IO[Throwable, A] =
    ZIO.scoped {
      HttpClientZioBackend.scoped().flatMap(backend => request(backend))
    }

  /**
   * Get response body from Response and narrow error type to [[SttpErr]].
   */
  extension [A](requestIO: Task[Response[Either[String, A]]]) {
    def flattenBody: IO[SttpErr, A] =
      requestIO
        .mapError(NotPass.apply)
        .flatMap { rsp =>
          rsp.code match
            case StatusCode.NotFound => ZIO.fail(NotFound)
            case _                   => ZIO.fromEither(rsp.body).mapError(NotOk.apply)
        }
  }

  /**
   * Get response body from Response and narrow error type to [[SttpErr]].
   */
  extension [A](requestIO: Task[Response[Either[ResponseException[String, String], A]]]) {
    def flattenBodyT: IO[SttpErr, A] =
      requestIO
        .mapError(NotPass.apply)
        .flatMap { rsp =>
          rsp.code match
            case StatusCode.NotFound => ZIO.fail(NotFound)
            case _                   => ZIO.fromEither(rsp.body).mapError(NotOkT.apply)
        }
  }

  /**
   * Handle string response body and narrow error type.
   */
  extension [A](requestIO: Task[String]) {
    def attemptBody(f: String => A): IO[Throwable, A] =
      requestIO.flatMap { body => ZIO.attempt(f(body)) }
  }

  /**
   * Collapse the Either left in the RequestT to the ZIO error channel.
   */
  implicit class RequestBodyIOWrapper[A](requestIO: IO[Throwable, Either[String, A]]) {
    def narrowEither: IO[Throwable, A] = requestIO.flatMap {
      case Left(err)  => ZIO.fail(NotOk(err))
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
