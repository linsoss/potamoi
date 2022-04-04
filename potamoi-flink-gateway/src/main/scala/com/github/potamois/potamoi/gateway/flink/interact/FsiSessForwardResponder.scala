package com.github.potamois.potamoi.gateway.flink.interact

import akka.actor.typed._
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import com.github.potamois.potamoi.gateway.flink.interact.FsiExecutor._

/**
 * [[FsiSessManager.Forward]] proxy responder to [[FsiExecutor.Command]] carrying
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

  sealed trait Reply extends Command
  private case class ExecuteSqlsReply(reply: ActorRef[MaybeDone], rs: MaybeDone) extends Reply
  private case class IsInProcessReply(reply: ActorRef[Boolean], rs: Boolean) extends Reply
  private case class GetExecPlanRsSnapshotReply(reply: ActorRef[ExecPlanResult], rs: ExecPlanResult) extends Reply
  private case class GetQueryRsSnapshotReply(reply: ActorRef[QueryResult], rs: QueryResult) extends Reply
  private case class GetQueryRsSnapshotByPageReply(reply: ActorRef[PageQueryResult], rs: PageQueryResult) extends Reply

  /**
   * Proxy tell message to [[FsiExecutor.Command]], and receive reply if necessary.
   */
  private def proxyTell(executor: ActorRef[FsiExecutor.Command], command: FsiExecutor.Command)
                       (implicit ctx: ActorContext[Command]): Behavior[Command] = {
    command match {
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
      case _ =>
        executor ! command
    }
    Behaviors.same
  }

  /**
   * Proxy reply message to original ActorRef received from [[FsiExecutor.Command]].
   */
  private def proxyReply(cmd: Reply): Behavior[Command] = {
    cmd match {
      case ExecuteSqlsReply(reply, rs) => reply ! rs
      case IsInProcessReply(reply, rs) => reply ! rs
      case GetExecPlanRsSnapshotReply(reply, rs) => reply ! rs
      case GetQueryRsSnapshotReply(reply, rs) => reply ! rs
      case GetQueryRsSnapshotByPageReply(reply, rs) => reply ! rs
    }
    Behaviors.same
  }

  def apply(): Behavior[Command] = Behaviors.setup { implicit ctx =>
    ctx.log.info("Local FsiSessForwardResponder started.")
    Behaviors.supervise {
      Behaviors
        .receiveMessage[Command] {
          case ProxyTell(executor, command) => proxyTell(executor, command)
          case cmd: Reply => proxyReply(cmd)
        }
        .receiveSignal {
          case (_ctx, PostStop) =>
            _ctx.log.info("Local FsiSessForwardResponder stopped.")
            Behaviors.same
          case (_ctx, PreRestart) =>
            _ctx.log.info("Local FsiSessForwardResponder restarting...")
            Behaviors.same
        }
    }.onFailure(SupervisorStrategy.restart)
  }


}
