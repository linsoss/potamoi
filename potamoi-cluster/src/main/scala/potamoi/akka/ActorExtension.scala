package potamoi.akka

import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.util.Timeout
import potamoi.akka.ActorOpErr.AskFailure
import potamoi.syntax.toPrettyString
import potamoi.times.given_Conversion_ZIODuration_Timeout
import zio.{Duration, IO, UIO, ZIO}

import scala.annotation.targetName

/**
 * Typed actor operation extension.
 */
val actorOp = ActorExtension

object ActorExtension:

  extension [U](actor: ActorRef[U]) {

    @targetName("tellZIOSymbol")
    inline def !>(message: U): UIO[Unit] = tellZIO(message)

    @targetName("askZIOSymbol")
    inline def ?>[Res](reply: ActorRef[Res] => U, timeout: Option[Duration] = None)(using cradle: ActorCradle): IO[AskFailure, Res] =
      askZIO(reply, timeout)

    /**
     * Tell with zio.
     */
    inline def tellZIO(message: U): UIO[Unit] = ZIO.attempt(actor.tell(message)).ignore

    /**
     * Ask with zio.
     */
    inline def askZIO[Res](reply: ActorRef[Res] => U, timeout: Option[Duration] = None)(using cradle: ActorCradle): IO[AskFailure, Res] = {
      val askTimeout = timeout.map(given_Conversion_ZIODuration_Timeout).getOrElse(cradle.askTimeout)
      ZIO
        .fromFutureInterrupt { implicit ec => actor.ask[Res](reply)(askTimeout, cradle.scheduler) }
        .mapError(err => AskFailure(actor.path.toString, err))
    }

  }
