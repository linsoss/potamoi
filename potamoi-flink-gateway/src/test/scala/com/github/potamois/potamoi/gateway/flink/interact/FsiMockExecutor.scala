package com.github.potamois.potamoi.gateway.flink.interact

import akka.Done
import akka.actor.Address
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, PostStop}
import com.github.potamois.potamoi.akka.serialize.CborSerializable
import com.github.potamois.potamoi.commons.EitherAlias.success
import com.github.potamois.potamoi.gateway.flink.interact.FsiSessManager.SessionId

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Mock implementation of [[FsiExecutor]] for multi-node testing of [[FsiSessManager]]
 *
 * @author Al-assad
 */
object FsiMockExecutor {

  import FsiExecutor._
  import NodeFsiSessObserver._

  val Created = "create"
  val Stopped = "stop"

  def apply(sessionId: SessionId,
            nodeCollector: ActorRef[NodeFsiSessObserver.Command]): Behavior[FsiExecutor.Command] = Behaviors.setup { ctx =>
    ctx.log.info(s"FsiMockExecutor[$sessionId] created.")
    nodeCollector ! ReceiveCommand(ctx.system.address, sessionId, Created)
    Behaviors
      .receiveMessage[FsiExecutor.Command] {
        case ExecuteSqls(sqls, _, replyTo) =>
          ctx.log.info(s"FsiMockExecutor[$sessionId] received ExecuteSqls[$sqls].")
          nodeCollector ! ReceiveCommand(ctx.system.address, sessionId, sqls)
          replyTo ! success(Done)
          Behaviors.same

        case Terminate(reason) =>
          ctx.log.info(s"FsiMockExecutor[$sessionId] received Terminate $reason.")
          Behaviors.stopped

        case _ => Behaviors.same
      }
      .receiveSignal {
        case (_, PostStop) =>
          ctx.log.info(s"FsiMockExecutor[$sessionId] stopped.")
          nodeCollector ! ReceiveCommand(ctx.system.address, sessionId, Stopped)
          Behaviors.same
      }
  }
}


object NodeFsiSessObserver {

  sealed trait Command extends CborSerializable
  case class ReceiveCommand(addr: Address, sessionId: String, cmd: String) extends Command
  case class GetSessHistory(sessionId: String, reply: ActorRef[Option[(Address, String)]]) extends Command
  case object Clear extends Command

  def apply(): Behavior[Command] = Behaviors.setup { ctx =>
    val history = mutable.Map.empty[SessionId, ListBuffer[(Address, String)]]

    Behaviors.receiveMessage[Command] {

      case ReceiveCommand(addr, sessId, cmd) =>
        if (history.contains(sessId)) history(sessId) += addr -> cmd
        else history += (sessId -> ListBuffer(addr -> cmd))
        Behaviors.same

      case GetSessHistory(sessId, reply) =>
        reply ! history.getOrElse(sessId, ListBuffer.empty).lastOption
        Behaviors.same

      case Clear =>
        history.clear()
        Behaviors.same
    }
  }
}

