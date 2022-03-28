package com.github.potamois.potamoi.gateway.flink

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

/**
 * Flink Sql interaction executor manager.
 *
 * @author Al-assad
 */
object FsiSessManager {

  type SessionId = String


  sealed trait Command


  sealed trait Internal extends Command


  def apply(): Behavior[Command] = Behaviors.setup { ctx =>

    Behaviors.receiveMessage {
      case _ => Behaviors.same
    }
  }


}
