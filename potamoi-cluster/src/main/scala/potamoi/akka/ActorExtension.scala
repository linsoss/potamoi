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

val actors    = ActorExtension
val behaviors = BehaviorExtension

case class ActorOpErr(actorPath: String, cause: Throwable) extends PotaErr

/**
 * Actor operation extension.
 */
object ActorExtension:
  extension [U](actor: ActorRef[U]) {

    @targetName("tellZIOSymbol")
    inline def !>(message: U): IO[ActorOpErr, Unit] = tellZIO(message)

    @targetName("askZIOSymbol")
    inline def ?>[Res](reply: ActorRef[Res] => U, timeout: Option[Duration] = None)(using cradle: ActorCradle): IO[ActorOpErr, Res] =
      askZIO(reply, timeout)

    /**
     * Tell with zio.
     */
    inline def tellZIO(message: U): IO[ActorOpErr, Unit] = {
      ZIO
        .attempt(actor.tell(message))
        .mapError(err => ActorOpErr(actor.path.toString, err))
    }

    /**
     * Ask with zio.
     */
    inline def askZIO[Res](reply: ActorRef[Res] => U, timeout: Option[Duration] = None)(using cradle: ActorCradle): IO[ActorOpErr, Res] = {
      val askTimeout = timeout.map(given_Conversion_ZIODuration_Timeout).getOrElse(cradle.askTimeout)
      ZIO
        .fromFutureInterrupt { implicit ec => actor.ask[Res](reply)(askTimeout, cradle.scheduler) }
        .mapError(err => ActorOpErr(actor.path.toString, err))
    }
  }

/**
 * Actor Behavior enhancement.
 */
object BehaviorExtension:
  extension [U](behavior: Behavior[U]) {

    /**
     * Behaviors.supervise.onFailure
     */
    inline def onFailure[Thr <: Throwable](strategy: SupervisorStrategy)(implicit tag: ClassTag[Thr] = ClassTag(classOf[Throwable])): Behavior[U] = {
      Behaviors.supervise(behavior).onFailure[Thr](strategy)
    }

    /**
     * Execute the function before the behavior begins.
     */
    inline def beforeIt(func: => Unit): Behavior[U] = {
      func
      behavior
    }
  }
