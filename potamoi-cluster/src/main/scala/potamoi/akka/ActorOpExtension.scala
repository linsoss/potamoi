package potamoi.akka

import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import potamoi.akka.ActorOpErr
import potamoi.syntax.toPrettyString
import potamoi.times.given_Conversion_ZIODuration_Timeout
import potamoi.PotaErr
import zio.{Duration, IO, UIO, ZIO}

import scala.annotation.targetName
import scala.reflect.ClassTag
import scala.util.Try

case class ActorOpErr(actorPath: String, message: String = "", cause: Throwable) extends PotaErr

/**
 * Actor operation extension.
 */
object ActorOpExtension:
  extension [U](actor: ActorRef[U]) {

    @targetName("tellZIOSymbol")
    inline def !>(message: U): IO[ActorOpErr, Unit] = tellZIO(message)

    @targetName("askZIOSymbol")
    inline def ?>[Res](reply: ActorRef[Res] => U, timeout: Option[Duration] = None)(using matrix: AkkaMatrix): IO[ActorOpErr, Res] =
      askZIO(reply, timeout)

    /**
     * Tell with zio.
     */
    inline def tellZIO(message: U): IO[ActorOpErr, Unit] = {
      ZIO
        .attempt(actor.tell(message))
        .mapError { err =>
          ActorOpErr(
            actorPath = actor.path.toString,
            message = Try(message.toString()).getOrElse(""),
            cause = err
          )
        }
    }

    /**
     * Ask with zio.
     */
    inline def askZIO[Res](
        reply: ActorRef[Res] => U,
        timeout: Option[Duration] = None
      )(using matrix: AkkaMatrix): IO[ActorOpErr, Res] = {

      val askTimeout = timeout.map(given_Conversion_ZIODuration_Timeout).getOrElse(matrix.askTimeout)
      val askEffect  = ZIO
        .fromFutureInterrupt { implicit ec => actor.ask[Res](reply)(askTimeout, matrix.scheduler) }
        .mapError { err =>
          ActorOpErr(
            actorPath = actor.path.toString,
            message = Try(reply(null).toString).getOrElse(""),
            cause = err
          )
        }
      ZIO.blocking(askEffect)
    }

  }

