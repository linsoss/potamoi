package com.github.potamois.potamoi.gateway.flink.interact


import akka.actor.typed._
import akka.actor.typed.receptionist.Receptionist.Registered
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.adapter.TypedActorContextOps
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, Routers, TimerScheduler}
import akka.util.Timeout
import com.github.potamois.potamoi.akka.serialize.CborSerializable
import com.github.potamois.potamoi.akka.toolkit.ActorImplicit
import com.github.potamois.potamoi.commons.EitherAlias.{fail, success}
import com.github.potamois.potamoi.commons.JdkDurationConversions.JavaDurationImplicit
import com.github.potamois.potamoi.commons.{PotaConfig, Uuid}
import com.github.potamois.potamoi.gateway.flink.FlinkVersion.{FlinkVerSign, FlinkVerSignRange, SystemFlinkVerSign}
import com.github.potamois.potamoi.gateway.flink.interact.FsiSessManager.{Command, SessionId}

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration, MILLISECONDS}
import scala.language.{implicitConversions, postfixOps}
import scala.reflect.runtime.universe.typeOf
import scala.util.Success

/**
 * Flink Sql interaction executor manager.
 *
 * @author Al-assad
 */
object FsiSessManager {

  type SessionId = String
  type MaybeSessionId = Either[CreateSessReqReject, SessionId]
  type FsiExecutorActor = Option[ActorRef[FsiExecutor.Command]]

  sealed trait Command extends CborSerializable

  private[interact] sealed trait CreateSessionCommand
  private[interact] sealed trait FindSessionCommand
  private[interact] sealed trait CloseSessionCommand

  /**
   * Create a new FsiExecutor session.
   *
   * @param flinkVer The anticipated flink major version sign, see [[FlinkVerSign]]
   * @param replyTo  It will reply the assigned session-id, or the reason for refusing to
   *                 assign a session-id in [[CreateSessReqReject]],
   *                 The assigned session-id is a 32-bit uuid string.
   */
  final case class CreateSession(flinkVer: FlinkVerSign, replyTo: ActorRef[MaybeSessionId]) extends Command with CreateSessionCommand

  /**
   * Create a new Executor locally and return the assigned session-id.
   */
  final case class CreateLocalSession(replyTo: ActorRef[MaybeSessionId]) extends Command with CreateSessionCommand

  /**
   * Find the FsiExecutor with the specified session-id.
   */
  final case class FindSession(sessionId: SessionId, reply: ActorRef[FsiExecutorActor]) extends Command with FindSessionCommand

  /**
   * Close the FisExecutor with the specified session-id.
   */
  final case class CloseSession(sessionId: SessionId) extends Command with CloseSessionCommand

  /**
   * Terminate the current FsiMessageManager instance gracefully.
   */
  final case object Terminate extends Command


  sealed trait Internal extends Command

  // Retryable CreatingSession command.
  private final case class RetryableCreateSession(retryCount: Int, flinkVer: FlinkVerSign,
                                                  replyTo: ActorRef[MaybeSessionId]) extends Internal with CreateSessionCommand

  // The session has been created successfully.
  private final case class CreatedLocalSession(sessId: SessionId,
                                               replyTo: ActorRef[MaybeSessionId]) extends Internal with CreateSessionCommand

  // Retryable FindSession command.
  private case class RetryableFindSession(retryCount: Int, sessionId: SessionId,
                                          reply: ActorRef[FsiExecutorActor]) extends Command with FindSessionCommand

  private case class RetryableFindSessionListing(retryCount: Int, sessionId: SessionId, reply: ActorRef[FsiExecutorActor],
                                                 listing: Receptionist.Listing) extends Internal with FindSessionCommand

  // Found FsiExecutor message adapter.
  private case class FoundSessionThenCloseAdapter(executor: FsiExecutorActor) extends Internal with CloseSessionCommand

  // Update the number of FsiSessManager service slots for the specified FlinkVerSign.
  private final case class UpdateSessManagerServiceSlots(flinkVer: FlinkVerSign, slotSize: Int) extends Internal


  // receptionist service key for FsiExecutor actor
  val FsiExecutorServiceKey: ServiceKey[FsiExecutor.Command] = ServiceKey[FsiExecutor.Command]("fsi-executor")

  // receptionist service keys for multi-type of FsiSessionManager actors
  val FsiSessManagerServiceKeys: Map[FlinkVerSign, ServiceKey[Command]] = FlinkVerSignRange.map(flinkVer =>
    flinkVer -> ServiceKey[Command](s"fsi-sess-manager-$flinkVer")).toMap

  /**
   * Default behavior creation.
   */
  def apply(): Behavior[Command] = apply(
    flinkVerSign = SystemFlinkVerSign,
    fsiExecutorBehavior = FsiSerialExecutor.apply
  )

  /**
   * Behavior creation
   *
   * @param flinkVerSign        Flink version sign of the current FisSessionManager,the flink version of the system
   *                            is used by default.
   * @param fsiExecutorBehavior The behavior of the FsiExecutor actor, use [[FsiSerialExecutor]] by default.
   * @param autoRestart         Whether to restart the FsiExecutor actor automatically when it fails.
   */
  def apply(flinkVerSign: FlinkVerSign = SystemFlinkVerSign,
            fsiExecutorBehavior: SessionId => Behavior[FsiExecutor.Command] = FsiExecutor.apply,
            autoRestart: Boolean = true): Behavior[Command] =
    Behaviors.setup[Command] { implicit ctx =>
      Behaviors.withTimers { implicit timers =>
        if (autoRestart) {
          Behaviors.supervise {
            ctx.log.info(s"FisSessionManager[$flinkVerSign] actor created, auto-restart enabled.")
            new FsiSessManager(flinkVerSign, fsiExecutorBehavior).action()
          }.onFailure(SupervisorStrategy.restart)
        } else {
          ctx.log.info(s"FisSessionManager[$flinkVerSign] actor created, auto-restart disabled.")
          new FsiSessManager(flinkVerSign, fsiExecutorBehavior).action()
        }
      }
    }

  /**
   * Get FsiExecutor actor name via sessionId.
   */
  def fsiExecutorName(sessionId: SessionId): String = s"fsi-executor-$sessionId"

}


class FsiSessManager private(flinkVer: FlinkVerSign,
                             fsiExecutorBehavior: SessionId => Behavior[FsiExecutor.Command])
                            (implicit ctx: ActorContext[Command], timers: TimerScheduler[Command]) extends ActorImplicit[Command] {

  import FsiSessManager._

  implicit val askTimeout: Timeout = 3.seconds

  // command retryable config
  private val retryPropCreateSession = RetryProp("potamoi.flink-gateway.sql-interaction.fsi-sess-cmd-retry.create-session")
  private val retryPropFindSession = RetryProp("potamoi.flink-gateway.sql-interaction.fsi-sess-cmd-retry.find-session")

  // FsiSessManagerServiceKeys listing state
  private val sessMgrServiceSlots: mutable.Map[FlinkVerSign, Int] = mutable.Map(FlinkVerSignRange.map(_ -> 0): _*)

  // subscribe receptionist listing of all FsiSessManagerServiceKeys
  FsiSessManagerServiceKeys.foreach { case (flinkVer, serviceKey) =>
    val subscriber = ctx.spawn(SessManagerServiceSubscriber(flinkVer), s"fsi-sess-manager-subscriber-$flinkVer")
    receptionist ! Receptionist.Subscribe(serviceKey, subscriber)
  }

  // FsiSessManager group routers
  private val sessMgrServiceRoutes: Map[FlinkVerSign, ActorRef[Command]] = FsiSessManagerServiceKeys.map {
    case (flinkVer, serviceKey) =>
      val group = Routers.group(serviceKey).withRoundRobinRouting
      val sessMgrGroup = ctx.spawn(group, s"fsi-sess-manager-$flinkVer")
      flinkVer -> sessMgrGroup
  }

  // register FsiSessManagerServiceKeys to receptionist
  receptionist ! Receptionist.Register(FsiSessManagerServiceKeys(flinkVer), ctx.self)

  /**
   * Received message behaviors.
   */
  private def action(): Behavior[Command] = Behaviors.receiveMessage[Command] {

    case cmd: FindSessionCommand => cmd match {
      case FindSession(sessionId, reply) =>
        ctx.self ! RetryableFindSession(1, sessionId, reply)
        Behaviors.same

      case RetryableFindSession(retryCount, sessionId, reply) =>
        receptionist ! FsiExecutorServiceKey -> (RetryableFindSessionListing(retryCount, sessionId, reply, _))
        Behaviors.same

      case RetryableFindSessionListing(retryCount, sessionId, reply, listing) =>
        listing.serviceInstances(FsiExecutorServiceKey).find(_.path.name == fsiExecutorName(sessionId)) match {
          case Some(executor) => reply ! Some(executor)
          case None =>
            if (retryCount >= retryPropFindSession.limit) reply ! None
            else timers.startSingleTimer(
              key = s"$sessionId-fs-${Uuid.genUUID16}",
              RetryableFindSession(retryCount + 1, sessionId, reply),
              retryPropFindSession.interval)
        }
        Behaviors.same
    }

    // create session command
    case cmd: CreateSessionCommand => cmd match {
      case CreateSession(flinkVer, replyTo) =>
        ctx.self ! RetryableCreateSession(1, flinkVer, replyTo)
        Behaviors.same

      case RetryableCreateSession(retryCount, flinkVer, replyTo) =>
        if (retryCount > retryPropCreateSession.limit) replyTo ! fail(NoActiveFlinkGatewayService(flinkVer))
        else flinkVer match {
          case ver if !FlinkVerSignRange.contains(ver) => replyTo ! fail(UnsupportedFlinkVersion(flinkVer))
          case ver if sessMgrServiceSlots.getOrElse(ver, 0) < 1 =>
            timers.startSingleTimer(
              key = s"$flinkVer-ct-${Uuid.genUUID16}",
              RetryableCreateSession(retryCount + 1, flinkVer, replyTo),
              retryPropCreateSession.interval)
          case ver => sessMgrServiceRoutes(ver) ! CreateLocalSession(replyTo)
        }
        Behaviors.same

      case CreateLocalSession(replyTo) =>
        val sessionId = Uuid.genUUID32
        // create FsiExecutor actor
        val executor = ctx.spawn(fsiExecutorBehavior(sessionId), fsiExecutorName(sessionId))
        ctx.watch(executor)
        receptionist ! Receptionist.register(
          FsiExecutorServiceKey, executor,
          messageAdapter[Registered](_ => CreatedLocalSession(sessionId, replyTo))
        )
        Behaviors.same

      case CreatedLocalSession(sessionId, replyTo) =>
        ctx.log.info(s"FsiSessManager[$flinkVer] register FsiExecutor[${fsiExecutorName(sessionId)}] actor to receptionist.")
        replyTo ! success(sessionId)
        Behaviors.same
    }

    case cmd: CloseSessionCommand => cmd match {
      case CloseSession(sessionId) =>
        ctx.ask[FindSession, FsiExecutorActor](ctx.self, ref => FindSession(sessionId, ref)) {
          case Success(executor) => FoundSessionThenCloseAdapter(executor)
          case _ => FoundSessionThenCloseAdapter(None)
        }
        Behaviors.same

      case FoundSessionThenCloseAdapter(executor) =>
        executor match {
          case Some(actor) => actor ! FsiExecutor.Terminate("via CloseSession of parent.")
          case _ =>
        }
        Behaviors.same
    }

    case UpdateSessManagerServiceSlots(flinkVer, slotSize) =>
      sessMgrServiceSlots(flinkVer) = slotSize
      Behaviors.same

    case Terminate =>
      ctx.log.info(s"FsiSessManager[$flinkVer] begins a graceful termination.")
      // stopped all local FsiExecutor actors gracefully.
      ctx.children
        .filter(child => typeOf[child.type] == typeOf[FsiExecutor.Command])
        .foreach {
          _.asInstanceOf[ActorRef[FsiExecutor.Command]] ! FsiExecutor.Terminate("the parent manager is terminating.")
        }
      Behaviors.stopped

  }.receiveSignal {
    // when receive the termination signal of executor, deregister it from receptionist
    case (_, Terminated(actor: ActorRef[FsiExecutor.Command])) =>
      receptionist ! Receptionist.deregister(FsiExecutorServiceKey, actor)
      ctx.log.info(s"FsiSessManager[$flinkVer] deregister FsiExecutor[${actor.path.name}] actor from receptionist.")
      Behaviors.same

    case (_ctx, PreRestart) =>
      _ctx.log.info(s"FsiSessManager[$flinkVer] restarting.")
      Behaviors.same

    case (_ctx, PostStop) =>
      _ctx.log.info(s"FsiSessManager[$flinkVer] stopped.")
      Behaviors.same
  }


  /**
   * Subscribe to receptionist listing of FsiSessManagerServiceKeys,
   * this actor can only be used by [[FsiSessManager]].
   */
  private object SessManagerServiceSubscriber {
    def apply(flinkVer: FlinkVerSign): Behavior[Receptionist.Listing] = Behaviors.supervise {
      Behaviors.receive[Receptionist.Listing] { (context, listing) =>
        val instances = listing.serviceInstances(FsiSessManagerServiceKeys(flinkVer))
        context.toClassic.parent ! UpdateSessManagerServiceSlots(flinkVer, instances.size)
        Behaviors.same
      }
    }.onFailure(SupervisorStrategy.restart)
  }

  /**
   * @param limit    max retry count
   * @param interval interval between retries
   */
  private case class RetryProp(limit: Int, interval: FiniteDuration)
  private object RetryProp {
    def apply(path: String): RetryProp = RetryProp(
      limit = PotaConfig.root.getInt(s"$path.limit"),
      interval = PotaConfig.root.getDuration(s"$path.interval").asScala(MILLISECONDS)
    )
  }

}
