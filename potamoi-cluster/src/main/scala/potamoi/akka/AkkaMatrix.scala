package potamoi.akka

import akka.actor.typed.*
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.typed.Cluster
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.util.Timeout
import com.typesafe.config.Config
import potamoi.times.{given_Conversion_ScalaDuration_Timeout, given_Conversion_ZIODuration_Timeout}
import potamoi.EarlyLoad
import potamoi.akka.actors.*
import potamoi.cluster.{ClusterSnapshot, Member}
import zio.{durationInt, Duration, IO, Scope, Task, URIO, ZIO, ZLayer}

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.jdk.CollectionConverters.*

/**
 * Typed actor system wrapper.
 */
object AkkaMatrix extends EarlyLoad[AkkaMatrix] {

  val live: ZLayer[Scope with Config with AkkaConf, Throwable, AkkaMatrix] = ZLayer {
    for {
      akkaConf <- ZIO.service[AkkaConf]
      config   <- akkaConf.resolveConfig
      system   <- ZIO.acquireRelease {
                    ZIO.attempt(ActorSystem(ActorMatrixActor(), akkaConf.systemName, config))
                  } { system =>
                    ZIO
                      .attempt(system.terminate())
                      .tapErrorCause(ZIO.logErrorCause("Failed to terminate actor system", _))
                      .ignore
                  }
      _        <- ZIO
                    .attempt(system.settings.config.getStringList("akka.cluster.roles").asScala.mkString(","))
                    .flatMap(s => ZIO.logInfo(s"Actor system roles: $s"))
                    .ignore
    } yield AkkaMatrix(akkaConf, system)
  }.tapErrorCause(ZIO.logErrorCause("Failed to create actor system", _))

  override def active: URIO[AkkaMatrix, Unit] =
    for {
      actorCradle <- ZIO.service[AkkaMatrix]
      _           <- ZIO.logInfo(s"Activate akka actor system: ${actorCradle.name}")
    } yield ()

}

class AkkaMatrix(akkaConf: AkkaConf, val system: ActorSystem[ActorMatrixActor.Event]) {

  lazy val spawnTimeout: Timeout = akkaConf.defaultSpawnTimeout
  lazy val askTimeout: Timeout   = akkaConf.defaultAskTimeout
  lazy val name: String          = system.name
  lazy val scheduler: Scheduler  = system.scheduler

  private given AkkaMatrix = this

  /**
   * Spawn actor.
   */
  def spawn[U](name: String, behavior: Behavior[U], props: Props = Props.empty, timeout: Option[Duration] = None): Task[ActorRef[U]] = {
    val askTimeout: Timeout = timeout.map(given_Conversion_ZIODuration_Timeout).getOrElse(spawnTimeout)
    ZIO.fromFuture { implicit ec =>
      system.ask[ActorRef[U]](res => ActorMatrixActor.Spawn(behavior, name, props, res))(askTimeout, system.scheduler)
    }
  }

  /**
   * Spawn actor anonymously.
   */
  def spawnAnonymous[U](behavior: Behavior[U], props: Props = Props.empty, timeout: Option[Duration] = None): Task[ActorRef[U]] = {
    val askTimeout: Timeout = timeout.map(given_Conversion_ZIODuration_Timeout).getOrElse(spawnTimeout)
    ZIO.fromFuture { implicit ec =>
      system.ask[ActorRef[U]](res => ActorMatrixActor.SpawnAnonymous(behavior, props, res))(askTimeout, system.scheduler)
    }
  }

  /**
   * Stop actor.
   */
  def stop[U](actor: ActorRef[U]): Task[Unit] = ZIO.attempt {
    system ! ActorMatrixActor.Stop(actor)
  }

  /**
   * Find receptionist from system.
   */
  def findReceptionist[U](
      key: ServiceKey[U],
      includeUnreachable: Boolean = false,
      timeout: Option[Duration] = None): IO[ActorOpErr, Set[ActorRef[U]]] = {
    system.receptionist
      .askZIO(Receptionist.Find(key), timeout)
      .map { listing => if includeUnreachable then listing.allServiceInstances(key) else listing.serviceInstances(key) }
  }

  /**
   * Get details about this akka cluster node itself.
   */
  def selfMember: IO[ActorOpErr, Member] = system.askZIO(ActorMatrixActor.GetSelfMember.apply)

  /**
   * List all cluster members node info of this akka system.
   */
  def listMembers: IO[ActorOpErr, Set[Member]] = system.askZIO(ActorMatrixActor.GetAllMembers.apply)

  /**
   * Get current cluster state of this akka system.
   */
  def clusterSnapshot: IO[ActorOpErr, ClusterSnapshot] = system.askZIO(ActorMatrixActor.GetClusterSnapshot.apply)

}

/**
 * Akka system root actor behavior.
 */
object ActorMatrixActor {

  sealed trait Event

  case class Spawn[U](behavior: Behavior[U], name: String, props: Props = Props.empty, reply: ActorRef[ActorRef[U]]) extends Event
  case class SpawnAnonymous[U](behavior: Behavior[U], props: Props = Props.empty, reply: ActorRef[ActorRef[U]])      extends Event
  case class Stop[U](actor: ActorRef[U])                                                                             extends Event
  case class GetSelfMember(reply: ActorRef[Member])                                                                  extends Event
  case class GetAllMembers(reply: ActorRef[Set[Member]])                                                             extends Event
  case class GetClusterSnapshot(reply: ActorRef[ClusterSnapshot])                                                    extends Event

  def apply(): Behavior[Event] = Behaviors.setup { ctx =>
    ctx.log.info("Potamoi ActorCradle started.")
    val cluster = Cluster(ctx.system)
    cluster.manager

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

      case GetSelfMember(reply) =>
        reply ! Member(cluster.selfMember)
        Behaviors.same

      case GetAllMembers(reply) =>
        reply ! cluster.state.getMembers.asScala.map(Member(_)).toSet
        Behaviors.same

      case GetClusterSnapshot(reply) =>
        reply ! ClusterSnapshot(cluster.selfMember, cluster.state)
        Behaviors.same
    }
  }
}
