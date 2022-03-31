package com.github.potamois.potamoi.gateway.flink.interact


import akka.actor.typed._
import akka.actor.typed.receptionist.Receptionist.Registered
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.adapter.TypedActorContextOps
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, Routers, TimerScheduler}
import com.github.potamois.potamoi.akka.toolkit.ActorImplicit
import com.github.potamois.potamoi.akka.toolkit.ActorImplicit.log
import com.github.potamois.potamoi.commons.EitherAlias.{fail, success}
import com.github.potamois.potamoi.commons.{CborSerializable, Uuid}
import com.github.potamois.potamoi.gateway.flink.FlinkVersion.{FlinkVerSign, FlinkVerSignRange, SystemFlinkVerSign}
import com.github.potamois.potamoi.gateway.flink.interact.FsiSessManager.{Command, SessionId}

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.language.implicitConversions

/**
 * Flink Sql interaction executor manager.
 *
 * @author Al-assad
 */
object FsiSessManager {

  type SessionId = String
  type RejectOrSessionId = Either[CreateSessReqReject, SessionId]
  type IsForwardAck = Boolean

  sealed trait Command extends CborSerializable

  private[interact] sealed trait CreateSessionCommand
  private[interact] sealed trait ForwardCommand
  private[interact] sealed trait SessionCommand
  private[interact] sealed trait ExistSessionCommand

  /**
   * Create a new FsiExecutor session.
   *
   * @param flinkVer The anticipated flink major version sign, see [[FlinkVerSign]]
   * @param replyTo  It will reply the assigned session-id, or the reason for refusing to
   *                 assign a session-id in [[CreateSessReqReject]],
   *                 The assigned session-id is a 32-bit uuid string.
   */
  final case class CreateSession(flinkVer: FlinkVerSign, replyTo: ActorRef[RejectOrSessionId])
    extends Command with CreateSessionCommand
  /**
   * Create a new Executor locally and return the assigned session-id.
   */
  final case class CreateLocalSession(replyTo: ActorRef[RejectOrSessionId])
    extends Command with CreateSessionCommand
  /**
   * Forward the FsiExecutor Command to the FsiExecutor with the corresponding session-id.
   */
  final case class Forward(sessionId: SessionId, executorCommand: FsiExecutor.Command)
    extends Command with ForwardCommand
  /**
   * Forward the FsiExecutor Command to the FsiExecutor with the corresponding session-id,
   * but returns an ack of whether the forwarding was successful.
   */
  final case class ForwardWithAck(sessionId: SessionId, executorCommand: FsiExecutor.Command, ackReply: ActorRef[IsForwardAck])
    extends Command with ForwardCommand
  /**
   * Determine if the specified session-id FsiExecutor exist.
   */
  final case class ExistSession(sessionId: SessionId, replyTo: ActorRef[Boolean])
    extends Command with ExistSessionCommand
  /**
   * Close the FisExecutor with the specified session-id.
   */
  final case class CloseSession(sessionId: SessionId) extends Command
  /**
   * Terminate the current FsiMessageManager instance gracefully.
   */
  final case object Terminate extends Command


  // auto conversion for Forward
  implicit def forwardConversion(cmd: (SessionId, FsiExecutor.Command)): Forward = Forward(cmd._1, cmd._2)

  // auto conversion for ForwardWithAck
  implicit def forwardWithAckConversion(cmd: ((SessionId, FsiExecutor.Command), ActorRef[IsForwardAck])): ForwardWithAck =
    ForwardWithAck(cmd._1._1, cmd._1._2, cmd._2)


  sealed trait Internal extends Command

  // Retryable CreatingSession command.
  private final case class RetryableCreateSession(retryCount: Int, flinkVer: FlinkVerSign, replyTo: ActorRef[RejectOrSessionId])
    extends Internal with CreateSessionCommand

  // The session has been created successfully.
  private final case class CreatedLocalSession(sessId: SessionId, replyTo: ActorRef[RejectOrSessionId])
    extends Internal with CreateSessionCommand

  // Retryable forwarding FsiExecutor command.
  private final case class RetryableForward(retryCount: Int, forward: ForwardWithAck)
    extends Internal with ForwardCommand

  private final case class RetryableForwardListing(retryCount: Int, forward: ForwardWithAck, listing: Receptionist.Listing)
    extends Internal with ForwardCommand

  // Retryable ExistSession command.
  private final case class RetryableExistSession(retryCount: Int, exist: ExistSession)
    extends Internal with ExistSessionCommand

  private final case class RetryableExistSessionListing(retryCount: Int, exist: ExistSession, listing: Receptionist.Listing)
    extends Internal with ExistSessionCommand

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
            log.info(s"FisSessionManager[$flinkVerSign] actor created, auto-restart enabled.")
            new FsiSessManager(flinkVerSign, fsiExecutorBehavior).action()
          }.onFailure(SupervisorStrategy.restart)

        } else {
          log.info(s"FisSessionManager[$flinkVerSign] actor created, auto-restart disabled.")
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

  // command retryable config
  private val retryPropCreateSession = RetryProp(limit = 3, interval = 300.millis)
  private val retryPropExistSession = RetryProp(limit = 3, interval = 300.millis)
  private val retryPropForward = RetryProp(limit = 5, interval = 300.millis)

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
      ctx.watch(sessMgrGroup)
      flinkVer -> sessMgrGroup
  }

  // register FsiSessManagerServiceKeys to receptionist
  receptionist ! Receptionist.Register(FsiSessManagerServiceKeys(flinkVer), ctx.self)

  /**
   * Received message behaviors.
   */
  private def action(): Behavior[Command] = Behaviors.receiveMessage[Command] {

    // create session command
    case cmd: CreateSessionCommand => cmd match {
      case CreateSession(flinkVer, replyTo) =>
        ctx.self ! RetryableCreateSession(1, flinkVer, replyTo)
        Behaviors.same

      case RetryableCreateSession(retryCount, flinkVer, replyTo) =>
        if (retryCount > retryPropCreateSession.limit)
          replyTo ! fail(NoActiveFlinkGatewayService(flinkVer))
        else flinkVer match {
          case ver if !FlinkVerSignRange.contains(ver) => replyTo ! fail(UnsupportedFlinkVersion(flinkVer))
          case ver if sessMgrServiceSlots.getOrElse(ver, 0) < 1 =>
            timers.startSingleTimer(RetryableCreateSession(retryCount + 1, flinkVer, replyTo), retryPropCreateSession.interval)
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
        replyTo ! success(sessionId)
        Behaviors.same
    }

    // forward command
    case cmd: ForwardCommand => cmd match {
      case Forward(sessionId, command) =>
        ctx.self ! RetryableForward(1, ForwardWithAck(sessionId, command, ctx.system.ignoreRef))
        Behaviors.same

      case c: ForwardWithAck =>
        ctx.self ! RetryableForward(1, c)
        Behaviors.same

      case RetryableForward(retryCount, forward) =>
        receptionist ! FsiExecutorServiceKey -> (RetryableForwardListing(retryCount, forward, _))
        Behaviors.same

      case RetryableForwardListing(retryCount, forward, listing) =>
        listing.serviceInstances(FsiExecutorServiceKey).find(_.path.name == fsiExecutorName(forward.sessionId)) match {
          case Some(executor) =>
            executor ! forward.executorCommand
            forward.ackReply ! true
          case None =>
            if (retryCount > retryPropForward.limit) forward.ackReply ! false
            else timers.startSingleTimer(RetryableForward(retryCount + 1, forward), retryPropForward.interval)
        }
        Behaviors.same
    }

    // exist session command
    case cmd: ExistSessionCommand => cmd match {
      case c: ExistSession =>
        ctx.self ! RetryableExistSession(1, c)
        Behaviors.same

      case RetryableExistSession(retryCount, c) =>
        receptionist ! FsiExecutorServiceKey -> (RetryableExistSessionListing(retryCount, c, _))
        Behaviors.same

      case RetryableExistSessionListing(retryCount, c, listing) =>
        listing.serviceInstances(FsiExecutorServiceKey).find(_.path.name == fsiExecutorName(c.sessionId)) match {
          case Some(_) => c.replyTo ! true
          case None =>
            if (retryCount > retryPropExistSession.limit) c.replyTo ! false
            else timers.startSingleTimer(RetryableExistSession(retryCount + 1, c), retryPropExistSession.interval)
        }
        Behaviors.same
    }


    case CloseSession(sessionId) =>
      ctx.self ! Forward(sessionId, FsiExecutor.Terminate("via FsiSessManager's CloseSession command"))
      Behaviors.same

    case UpdateSessManagerServiceSlots(flinkVer, slotSize) =>
      sessMgrServiceSlots(flinkVer) = slotSize
      Behaviors.same

    case Terminate =>
      log.info(s"FsiSessManager[$flinkVer] begins a graceful termination.")
      ctx.children.foreach(ctx.stop)
      Behaviors.stopped

  }.receiveSignal {
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

}
