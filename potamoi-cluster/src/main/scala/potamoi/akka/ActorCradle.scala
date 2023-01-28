package potamoi.akka

import akka.actor.typed.*
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import com.typesafe.config.Config
import potamoi.times.{given_Conversion_ScalaDuration_Timeout, given_Conversion_ZIODuration_Timeout}
import potamoi.EarlyLoad
import potamoi.akka.actorOp.*
import zio.{durationInt, Duration, IO, Scope, Task, URIO, ZIO, ZLayer}

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor

/**
 * Typed actor system wrapper.
 */
object ActorCradle extends EarlyLoad[ActorCradle]:

  val live: ZLayer[Scope with Config with AkkaConf, Throwable, ActorCradle] = ZLayer {
    for {
      akkaConf <- ZIO.service[AkkaConf]
      config   <- akkaConf.resolveConfig
      system   <- ZIO.acquireRelease {
                    ZIO.attempt(ActorSystem(ActorCradleActor(), akkaConf.systemName, config))
                  } { system =>
                    ZIO
                      .attempt(system.terminate())
                      .tapErrorCause(ZIO.logErrorCause("Failed to terminate actor system", _))
                      .ignore
                  }
    } yield ActorCradle(akkaConf, system)
  }

  override def active: URIO[ActorCradle, Unit] =
    for {
      actorCradle <- ZIO.service[ActorCradle]
      _           <- ZIO.logInfo(s"Activate akka actor system: ${actorCradle.name}")
    } yield ()

class ActorCradle(akkaConf: AkkaConf, val system: ActorSystem[ActorCradleActor.Event]):

  lazy val spawnTimeout: Timeout = akkaConf.defaultSpawnTimeout
  lazy val askTimeout: Timeout   = akkaConf.defaultAskTimeout
  lazy val name: String          = system.name
  lazy val scheduler: Scheduler  = system.scheduler

  private given ActorCradle = this

  /**
   * Spawn actor.
   */
  def spawn[U](name: String, behavior: Behavior[U], props: Props = Props.empty, timeout: Option[Duration] = None): Task[ActorRef[U]] = {
    val askTimeout: Timeout = timeout.map(given_Conversion_ZIODuration_Timeout).getOrElse(spawnTimeout)
    ZIO.fromFuture { implicit ec =>
      system.ask[ActorRef[U]](res => ActorCradleActor.Spawn(behavior, name, props, res))(askTimeout, system.scheduler)
    }
  }

  /**
   * Spawn actor anonymously.
   */
  def spawnAnonymous[U](behavior: Behavior[U], props: Props = Props.empty, timeout: Option[Duration] = None): Task[ActorRef[U]] = {
    val askTimeout: Timeout = timeout.map(given_Conversion_ZIODuration_Timeout).getOrElse(spawnTimeout)
    ZIO.fromFuture { implicit ec =>
      system.ask[ActorRef[U]](res => ActorCradleActor.SpawnAnonymous(behavior, props, res))(askTimeout, system.scheduler)
    }
  }

  /**
   * Stop actor.
   */
  def stop[U](actor: ActorRef[U]): Task[Unit] = ZIO.attempt {
    system ! ActorCradleActor.Stop(actor)
  }

  /**
   * Find receptionist from system.
   */
  def findReceptionist[U](key: ServiceKey[U], includeUnreachable: Boolean = false): IO[ActorOpErr.AskFailure, Set[ActorRef[U]]] = {
    system.receptionist
      .askZIO(Receptionist.Find(key))
      .map { listing =>
        if includeUnreachable then listing.allServiceInstances(key) else listing.serviceInstances(key)
      }
  }

/**
 * Akka system root actor behavior.
 */
object ActorCradleActor:

  sealed trait Event
  case class Spawn[U](behavior: Behavior[U], name: String, props: Props = Props.empty, reply: ActorRef[ActorRef[U]]) extends Event
  case class SpawnAnonymous[U](behavior: Behavior[U], props: Props = Props.empty, reply: ActorRef[ActorRef[U]])      extends Event
  case class Stop[U](actor: ActorRef[U])                                                                             extends Event

  def apply(): Behavior[Event] = Behaviors.setup { ctx =>
    ctx.log.info("Potamoi ActorCradle started.")

    Behaviors.receiveMessage {
      case Spawn(behavior, name, props, reply) =>
        val actor = ctx.spawn(behavior, name, props)
        reply ! actor
        Behaviors.same

      case SpawnAnonymous(behavior, props, reply) =>
        val actor = ctx.spawnAnonymous(behavior, props)
        reply ! actor
        Behaviors.same

      case Stop(actor) =>
        ctx.stop(actor)
        Behaviors.same
    }
  }
