package com.github.potamois.potamoi.gateway.flink.interact

import akka.actor.typed.receptionist.Receptionist.Registered
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.adapter.TypedActorContextOps
import akka.actor.typed.scaladsl.{Behaviors, Routers}
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import com.github.potamois.potamoi.commons.EitherAlias.{fail, success}
import com.github.potamois.potamoi.commons.{CborSerializable, Uuid}
import com.github.potamois.potamoi.gateway.flink.FlinkVersion
import com.github.potamois.potamoi.gateway.flink.FlinkVersion.{FlinkVerSign, flinkVerSignRange}

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
 * Flink Sql interaction executor manager.
 *
 * @author Al-assad
 */
// todo refactor to class

object FsiSessManager {

  type SessionId = String
  type RejectOrSessionId = Either[CreateSessReqReject, SessionId]
  type IsForwardAck = Boolean

  sealed trait Command extends CborSerializable

  private sealed trait CreateSessionCommand
  private sealed trait ForwardCommand

  final case class CreateSession(flinkVer: FlinkVerSign, replyTo: ActorRef[RejectOrSessionId])
    extends Command with CreateSessionCommand

  final case class CreateLocalSession(replyTo: ActorRef[RejectOrSessionId])
    extends Command with CreateSessionCommand

  final case class Forward(sessionId: SessionId, executorCommand: FsiExecutor.Command)
    extends Command with ForwardCommand

  final case class ForwardWithAck(sessionId: SessionId, executorCommand: FsiExecutor.Command, ackReply: ActorRef[IsForwardAck])
    extends Command with ForwardCommand

  final case class CloseSession(sessionId: SessionId) extends Command

  sealed trait Internal extends Command

  private final case class CreatedLocalSession(sessId: SessionId, replyTo: ActorRef[RejectOrSessionId])
    extends Internal with CreateSessionCommand

  private final case class ForwardListing(forward: ForwardWithAck, listing: Receptionist.Listing)
    extends Internal with ForwardCommand

  private final case class RetryForward(retryCount: Int, forward: ForwardWithAck)
    extends Internal with ForwardCommand

  private final case class RetryForwardListing(retryCount: Int, forward: ForwardWithAck, listing: Receptionist.Listing)
    extends Internal with ForwardCommand

  private final case class UpdateSessManagerServiceSlots(flinkVer: FlinkVerSign, slotSize: Int) extends Internal


  // receptionist service key for FsiExecutor actor
  val FsiExecutorServiceKey: ServiceKey[FsiExecutor.Command] = ServiceKey[FsiExecutor.Command]("fsi-executor")

  // receptionist service keys for multi-type of FsiSessionManager actors
  val FsiSessManagerServiceKeys: Map[FlinkVerSign, ServiceKey[Command]] = flinkVerSignRange.map(flinkVer =>
    flinkVer -> ServiceKey[Command](s"fsi-sess-manager-$flinkVer")).toMap

  // todo read config from hocon
  private val forwardRetryProps = RetrySetting(limit = 5, interval = 300.milliseconds)

  /**
   * Get FsiExecutor actor name via sessionId.
   */
  def fsiExecutorName(sessionId: SessionId): String = s"fsi-executor-$sessionId"


  def apply(flinkVerSign: FlinkVerSign = FlinkVersion.curSystemFlinkVer.majorSign): Behavior[Command] = Behaviors.setup { ctx =>
    Behaviors.withTimers { timers =>

      // subscribe receptionist listing of all FsiSessManagerServiceKeys
      val sessMgrServiceSlots: mutable.Map[FlinkVerSign, Int] = mutable.Map(flinkVerSignRange.map(_ -> 0): _*)
      FsiSessManagerServiceKeys.foreach { case (flinkVer, serviceKey) =>
        val subscriber = ctx.spawn(SessManagerServiceSubscriber(flinkVer), s"fsi-sess-manager-subscriber-$flinkVer")
        ctx.system.receptionist ! Receptionist.Subscribe(serviceKey, subscriber)
      }

      // FsiSessManager group routers
      val sessMgrServiceRoutes = FsiSessManagerServiceKeys.map { case (flinkVer, serviceKey) =>
        val group = Routers.group(serviceKey).withRoundRobinRouting
        val sessMgrGroup = ctx.spawn(group, s"fsi-sess-manager-$flinkVer")
        ctx.watch(sessMgrGroup)
        flinkVer -> sessMgrGroup
      }

      ctx.system.receptionist ! Receptionist.Register(FsiSessManagerServiceKeys(flinkVerSign), ctx.self)

      Behaviors.receiveMessage {

        case cmd: CreateSessionCommand => cmd match {
          case CreateSession(flinkVer, replyTo) => flinkVer match {
            case ver if !flinkVerSignRange.contains(ver) =>
              replyTo ! fail(UnsupportedFlinkVersion(flinkVer))
              Behaviors.same
            case ver if sessMgrServiceSlots.getOrElse(ver, 0) < 1 =>
              replyTo ! fail(NoActiveFlinkGatewayService(flinkVer))
              Behaviors.same
            case ver =>
              sessMgrServiceRoutes(ver) ! CreateLocalSession(replyTo)
              Behaviors.same
          }

          case CreateLocalSession(replyTo) =>
            val sessionId = Uuid.genUUID32
            val executor = ctx.spawn(FsiSerialExecutor(sessionId), fsiExecutorName(sessionId))
            ctx.watch(executor)
            ctx.system.receptionist ! Receptionist.register(
              FsiExecutorServiceKey,
              executor,
              ctx.messageAdapter[Registered](_ => CreatedLocalSession(sessionId, replyTo))
            )
            Behaviors.same

          case CreatedLocalSession(sessionId, replyTo) =>
            replyTo ! success(sessionId)
            Behaviors.same
        }

        case cmd: ForwardCommand => cmd match {
          case Forward(sessionId, command) =>
            ctx.self ! ForwardWithAck(sessionId, command, ctx.system.ignoreRef)
            Behaviors.same

          case ForwardWithAck(sessionId, command, ackReply) =>
            ctx.system.receptionist ! Receptionist.Find(
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
            ctx.system.receptionist ! Receptionist.Find(
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
      }
    }
  }


  /**
   * @param limit    max retry count
   * @param interval interval between retries
   */
  private case class RetrySetting(limit: Int, interval: FiniteDuration)


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
