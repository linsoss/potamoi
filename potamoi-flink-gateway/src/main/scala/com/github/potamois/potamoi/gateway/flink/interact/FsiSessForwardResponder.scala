package com.github.potamois.potamoi.gateway.flink.interact

import akka.actor.typed._
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.adapter.TypedActorContextOps
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.potamois.potamoi.akka.toolkit.ActorImplicit.receptionist
import com.github.potamois.potamoi.gateway.flink.interact.FsiExecutor._
import com.github.potamois.potamoi.gateway.flink.interact.FsiSessManager.{FsiExecutorServiceKey, SessionId, fsiSessionIdFromActorRef}

import scala.collection.mutable.ListBuffer

/**
 * [[FsiSessManager.Forward]] proxy responder to [[FsiExecutor.Command]] with
 * ActorRef type reply prams, this actor works only in the local ActorSystem.
 *
 * For solving the problem that akka-typed does not serialize ActorRef well between
 * 3 different ActorSystems.
 *
 * See: [[https://discuss.lightbend.com/t/akka-typed-serialization/4336]]
 *
 * @author Al-assad
 */
object FsiSessForwardResponder {

  sealed trait Command
  final case class ProxyTell(executor: ActorRef[FsiExecutor.Command], command: FsiExecutor.Command) extends Command

  sealed trait ProxyReply extends Command

  private case class ExecuteSqlsReply(reply: ActorRef[MaybeDone], rs: MaybeDone) extends ProxyReply
  private case class IsInProcessReply(reply: ActorRef[Boolean], rs: Boolean) extends ProxyReply
  private case class GetExecPlanRsSnapshotReply(reply: ActorRef[ExecPlanResult], rs: ExecPlanResult) extends ProxyReply
  private case class GetQueryRsSnapshotReply(reply: ActorRef[QueryResult], rs: QueryResult) extends ProxyReply
  private case class GetQueryRsSnapshotByPageReply(reply: ActorRef[PageQueryResult], rs: PageQueryResult) extends ProxyReply

  private case class FsiExecutorShutdown(executor: Set[ActorRef[FsiExecutor.Command]]) extends Command


  def apply(): Behavior[Command] = Behaviors.setup {
    implicit ctx =>
      ctx.log.info("Local FsiSessForwardResponder started")

      implicit val rsChangeTopicBridges: ListBuffer[ListenerMapper] = ListBuffer.empty
      val subscriber = ctx.spawn(subscribeFsiExecutorServiceBehavior(), "fsi-exec-service-subscriber")
      receptionist ! Receptionist.Subscribe(FsiExecutorServiceKey, subscriber)

      def receiveBehavior = Behaviors
        .receiveMessage[Command] {
          case ProxyTell(executor, command) => proxyTell(executor, command)
          case cmd: ProxyReply => proxyReply(cmd)
          case FsiExecutorShutdown(sessionId) => executorsShutdown(sessionId)
        }
        .receiveSignal {
          case (_ctx, PostStop) =>
            _ctx.log.info("Local FsiSessForwardResponder stopped")
            Behaviors.same
          case (_ctx, PreRestart) =>
            _ctx.log.info("Local FsiSessForwardResponder restarting...")
            Behaviors.same
        }

      Behaviors.supervise(receiveBehavior).onFailure(SupervisorStrategy.restart)
  }


  private type OrigListener = ActorRef[ExecRsChange]
  private type ProxyListener = ActorRef[ExecRsChange]
  private case class ListenerMapper(sessId: SessionId, orig: OrigListener, proxy: ProxyListener)

  /**
   * Proxy tell message to [[FsiExecutor.Command]], and receive reply if necessary.
   */
  private def proxyTell(executor: ActorRef[FsiExecutor.Command], command: FsiExecutor.Command)
                       (implicit ctx: ActorContext[Command], rsTopicBridges: ListBuffer[ListenerMapper]): Behavior[Command] = {
    command match {

      // Proxy for all FsiExecutor.Command with ActorRef reply params
      case ExecuteSqls(sqlStatements, props, replyTo) =>
        executor ! ExecuteSqls(sqlStatements, props, ctx.messageAdapter(ExecuteSqlsReply(replyTo, _)))
      case IsInProcess(replyTo) =>
        executor ! IsInProcess(ctx.messageAdapter(IsInProcessReply(replyTo, _)))
      case GetExecPlanRsSnapshot(replyTo) =>
        executor ! GetExecPlanRsSnapshot(ctx.messageAdapter(GetExecPlanRsSnapshotReply(replyTo, _)))
      case GetQueryRsSnapshot(limit, replyTo) =>
        executor ! GetQueryRsSnapshot(limit, ctx.messageAdapter(GetQueryRsSnapshotReply(replyTo, _)))
      case GetQueryRsSnapshotByPage(page, replyTo) =>
        executor ! GetQueryRsSnapshotByPage(page, ctx.messageAdapter(GetQueryRsSnapshotByPageReply(replyTo, _)))

      // Proxy for SubscribeState and UnsubscribeState command.
      case SubscribeState(listener) =>
        val sessId = fsiSessionIdFromActorRef(executor)
        if (!rsTopicBridges.exists(e => e.sessId == sessId && e.orig == listener)) {
          val proxy = ctx.spawnAnonymous(ExecRsChangeBridge(listener))
          rsTopicBridges += ListenerMapper(sessId, listener, proxy)
          executor ! SubscribeState(proxy)
        }

      case UnsubscribeState(listener) =>
        val sessId = fsiSessionIdFromActorRef(executor)
        val proxies = rsTopicBridges.filter(e => e.sessId == sessId && e.orig == listener)
        proxies.foreach { e =>
          executor ! UnsubscribeState(e.proxy)
          ctx.stop(e.proxy)
        }
        rsTopicBridges --= proxies

      case _ =>
        executor ! command
    }
    Behaviors.same
  }

  private def executorsShutdown(executors: Set[ActorRef[FsiExecutor.Command]])
                               (implicit ctx: ActorContext[Command], rsTopicBridges: ListBuffer[ListenerMapper]): Behavior[Command] = {
    val sessIds = executors.map(fsiSessionIdFromActorRef)
    if (sessIds.nonEmpty) {
      val proxies = rsTopicBridges.filter(e => sessIds.contains(e.sessId))
      proxies.foreach(e => ctx.stop(e.proxy))
      rsTopicBridges --= proxies
    }
    Behaviors.same
  }

  /**
   * Proxy reply message to original ActorRef received from [[FsiExecutor.Command]].
   */
  private def proxyReply(cmd: ProxyReply): Behavior[Command] = {
    cmd match {
      case ExecuteSqlsReply(reply, rs) => reply ! rs
      case IsInProcessReply(reply, rs) => reply ! rs
      case GetExecPlanRsSnapshotReply(reply, rs) => reply ! rs
      case GetQueryRsSnapshotReply(reply, rs) => reply ! rs
      case GetQueryRsSnapshotByPageReply(reply, rs) => reply ! rs
    }
    Behaviors.same
  }

  /**
   * [[ExecRsChange]] forward bridge
   */
  private object ExecRsChangeBridge {
    def apply(origListener: ActorRef[ExecRsChange]): Behavior[ExecRsChange] =
      Behaviors.receiveMessage { event: ExecRsChange =>
        origListener ! event
        Behaviors.same
      }
  }

  /**
   * Subscribe the global FsiExecutor instances.
   */

  private def subscribeFsiExecutorServiceBehavior(): Behavior[Receptionist.Listing] = Behaviors.supervise {
    var preListing = Set.empty[ActorRef[FsiExecutor.Command]]

    Behaviors.receive[Receptionist.Listing] { (context, listing) =>
      val instances = listing.serviceInstances(FsiExecutorServiceKey)
      val shutdownExecutors = preListing -- (preListing & instances)
      context.toClassic.parent ! FsiExecutorShutdown(shutdownExecutors)
      preListing = instances
      Behaviors.same
    }
  }.onFailure(SupervisorStrategy.restart)

}



