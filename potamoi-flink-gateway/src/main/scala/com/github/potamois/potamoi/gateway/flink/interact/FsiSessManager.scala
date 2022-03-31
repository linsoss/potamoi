package com.github.potamois.potamoi.gateway.flink.interact


import akka.actor.typed.{ActorRef, Behavior, PostStop, PreRestart, SupervisorStrategy}
import akka.actor.typed.receptionist.Receptionist.Registered
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.adapter.TypedActorContextOps
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, Routers, TimerScheduler}
import com.github.potamois.potamoi.akka.toolkit.ActorImplicits.{log, receptionist}
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

  // todo exist session

  /**
   * Close the FisExecutor with the specified session-id.
   */
  final case class CloseSession(sessionId: SessionId) extends Command


  // auto conversion for Forward
  implicit def forwardConversion(cmd: (SessionId, FsiExecutor.Command)): Forward = Forward(cmd._1, cmd._2)

  // auto conversion for ForwardWithAck
  implicit def forwardWithAckConversion(cmd: ((SessionId, FsiExecutor.Command), ActorRef[IsForwardAck])): ForwardWithAck =
    ForwardWithAck(cmd._1._1, cmd._1._2, cmd._2)


  sealed trait Internal extends Command

  // Retry create session command
  private final case class RetryCreateSession(retryCount: Int, flinkVer: FlinkVerSign, replyTo: ActorRef[RejectOrSessionId])
    extends Internal with CreateSessionCommand

  // The session has been created successfully.
  private final case class CreatedLocalSession(sessId: SessionId, replyTo: ActorRef[RejectOrSessionId])
    extends Internal with CreateSessionCommand

  // Forward command carrying the FsiExecutorServiceKey receptionist list.
  private final case class ForwardListing(forward: ForwardWithAck, listing: Receptionist.Listing)
    extends Internal with ForwardCommand

  // Retry forwarding FsiExecutor command.
  private final case class RetryForward(retryCount: Int, forward: ForwardWithAck)
    extends Internal with ForwardCommand

  // RetryForward command carrying the FsiExecutorServiceKey receptionist list.
  private final case class RetryForwardListing(retryCount: Int, forward: ForwardWithAck, listing: Receptionist.Listing)
    extends Internal with ForwardCommand

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
            fsiExecutorBehavior: SessionId => Behavior[FsiExecutor.Command] = FsiSerialExecutor.apply,
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
                            (implicit ctx: ActorContext[Command], timers: TimerScheduler[Command]) {

  import FsiSessManager._

  //  todo read config from hocon
  // retry config: CreateSession command
  private val createSessionRetryProps = RetrySetting(limit = 3, interval = 300.millis)

  // retry config: Forward command
  private val forwardRetryProps = RetrySetting(limit = 5, interval = 300.millis)

  // subscribe receptionist listing of all FsiSessManagerServiceKeys
  private val sessMgrServiceSlots: mutable.Map[FlinkVerSign, Int] = mutable.Map(FlinkVerSignRange.map(_ -> 0): _*)

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
      case CreateSession(flinkVer, replyTo) => flinkVer match {
        case ver if !FlinkVerSignRange.contains(ver) =>
          replyTo ! fail(UnsupportedFlinkVersion(flinkVer))
          Behaviors.same
        case ver if sessMgrServiceSlots.getOrElse(ver, 0) < 1 =>
          timers.startSingleTimer(RetryCreateSession(1, flinkVer, replyTo), createSessionRetryProps.interval)
          Behaviors.same
        case ver =>
          sessMgrServiceRoutes(ver) ! CreateLocalSession(replyTo)
          Behaviors.same
      }

      case RetryCreateSession(retryCount, flinkVer, replyTo) =>
        if (retryCount > createSessionRetryProps.limit)
          replyTo ! fail(NoActiveFlinkGatewayService(flinkVer))
        else if (sessMgrServiceSlots.getOrElse(flinkVer, 0) >= 1)
          sessMgrServiceRoutes(flinkVer) ! CreateLocalSession(replyTo)
        else
          timers.startSingleTimer(RetryCreateSession(1, flinkVer, replyTo), createSessionRetryProps.interval)
        Behaviors.same

      case CreateLocalSession(replyTo) =>
        // todo check session is exists
        val sessionId = Uuid.genUUID32
        // create FsiExecutor actor
        // todo create actor as cluster singleton ?
        val executor = ctx.spawn(fsiExecutorBehavior(sessionId), fsiExecutorName(sessionId))
        ctx.watch(executor)
        receptionist ! Receptionist.register(
          FsiExecutorServiceKey,
          executor,
          ctx.messageAdapter[Registered](_ => CreatedLocalSession(sessionId, replyTo))
        )
        Behaviors.same

      case CreatedLocalSession(sessionId, replyTo) =>
        replyTo ! success(sessionId)
        Behaviors.same
    }

    // forward command
    case cmd: ForwardCommand => cmd match {
      case Forward(sessionId, command) =>
        ctx.self ! ForwardWithAck(sessionId, command, ctx.system.ignoreRef)
        Behaviors.same

      case ForwardWithAck(sessionId, command, ackReply) =>
        receptionist ! Receptionist.Find(
          FsiExecutorServiceKey,
          ctx.messageAdapter[Receptionist.Listing](ForwardListing(ForwardWithAck(sessionId, command, ackReply), _))
        )
        Behaviors.same

      case ForwardListing(forward, listing) =>
        listing.serviceInstances(FsiExecutorServiceKey).find(_.path.name == fsiExecutorName(forward.sessionId)) match {
          case Some(executor) =>
            executor ! forward.executorCommand
            forward.ackReply ! true
          case None => timers.startSingleTimer(RetryForward(1, forward), forwardRetryProps.interval)
        }
        Behaviors.same

      case RetryForward(retryCount, forward) =>
        receptionist ! Receptionist.Find(
          FsiExecutorServiceKey,
          ctx.messageAdapter[Receptionist.Listing](RetryForwardListing(retryCount, forward, _))
        )
        Behaviors.same

      case RetryForwardListing(retryCount, forward, listing) =>
        listing.serviceInstances(FsiExecutorServiceKey).find(_.path.name == fsiExecutorName(forward.sessionId)) match {
          case Some(executor) =>
            executor ! forward.executorCommand
            forward.ackReply ! true
          case None =>
            if (retryCount > forwardRetryProps.limit) forward.ackReply ! false
            else timers.startSingleTimer(RetryForward(retryCount + 1, forward), forwardRetryProps.interval)
        }
        Behaviors.same
    }

    case CloseSession(sessionId) =>
      ctx.self ! Forward(sessionId, FsiExecutor.Terminate("terminate executor via FsiSessManager's CloseSession command"))
      Behaviors.same

    case UpdateSessManagerServiceSlots(flinkVer, slotSize) =>
      sessMgrServiceSlots(flinkVer) = slotSize
      Behaviors.same

  }.receiveSignal {
    case (_ctx, PreRestart) =>
      _ctx.log.info(s"FsiSessManager[$flinkVer] restarting.")
      Behaviors.same
    case (_ctx, PostStop) =>
      // todo terminal all local executor
      _ctx.log.info(s"FsiSessManager[$flinkVer] stopped.")
      Behaviors.same
  }


  /**
   * @param limit    max retry count
   * @param interval interval between retries
   */
  private case class RetrySetting(limit: Int, interval: FiniteDuration)

  /**
   * Subscribe to receptionist listing of FsiSessManagerServiceKeys,
   * this actor can only be used by [[FsiSessManager]].
   */
  private object SessManagerServiceSubscriber {
    def apply(flinkVer: FlinkVerSign): Behavior[Receptionist.Listing] = Behaviors.supervise {
      Behaviors.receive[Receptionist.Listing] { (ctx, listing) =>
        val instances = listing.serviceInstances(FsiSessManagerServiceKeys(flinkVer))
        ctx.toClassic.parent ! UpdateSessManagerServiceSlots(flinkVer, instances.size)
        Behaviors.same
      }
    }.onFailure(SupervisorStrategy.restart)
  }

}
