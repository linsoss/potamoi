package com.github.potamois.potamoi.gateway.flink

import akka.actor.typed.Behavior
import akka.actor.typed.javadsl.Behaviors

/**
 * @author Al-assad
 */
object InteractSessionManager {

  sealed trait Command

  sealed trait Internal

  def apply(): Behavior[Command] = Behaviors.setup { ctx =>

    Behaviors.receiveMessage {
      case _ =>
        Behaviors.same
    }
  }

}
