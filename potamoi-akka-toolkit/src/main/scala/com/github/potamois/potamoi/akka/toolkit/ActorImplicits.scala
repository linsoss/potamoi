package com.github.potamois.potamoi.akka.toolkit

import akka.actor.typed.ActorRef
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.ActorContext
import org.slf4j.Logger

/**
 * Actor convenient implicit function, class.
 *
 * @author Al-assad
 */
object ActorImplicits {

  def log(implicit context: ActorContext[_]): Logger = context.log

  def receptionist(implicit context: ActorContext[_]): ActorRef[Receptionist.Command] = context.system.receptionist

}
