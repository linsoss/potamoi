package com.github.potamois.potamoi.akka.toolkit

import akka.actor.typed.ActorRef
import akka.actor.typed.receptionist.Receptionist.Listing
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.ActorContext
import org.slf4j.Logger

/**
 * Actor implicit conversion function, class.
 *
 * @author Al-assad
 */
object ActorImplicit {

  def log(implicit context: ActorContext[_]): Logger = context.log

  def receptionist(implicit context: ActorContext[_]): ActorRef[Receptionist.Command] = context.system.receptionist

  /**
   * Implicit conversion for [[Receptionist.Find]].
   * {{{
   *  // from
   *  receptionist ! Receptionist.Find(serviceKey, adapter)
   * // to
   * receptionist ! serviceKey -> adapter
   * }}}
   */
  def receptionistFindConversion[K, U](expression: (ServiceKey[K], ActorRef[Listing])): Receptionist.Command =
    Receptionist.find(expression._1, expression._2)
}


trait ActorImplicit[T] {

  def log(implicit context: ActorContext[T]): Logger = context.log

  def receptionist(implicit context: ActorContext[T]): ActorRef[Receptionist.Command] = context.system.receptionist

  /**
   * Implicit conversion for [[Receptionist.Find]].
   * {{{
   *  // from
   *  receptionist ! Receptionist.Find(serviceKey, context.adapter[Receptionist.Listing](adapterFunc))
   *  // to
   *  receptionist ! serviceKey -> adapterFunc
   * }}}
   */
  implicit def receptionistFindConversionFromAdapter[K](expression: (ServiceKey[K], Receptionist.Listing => T))
                                                       (implicit context: ActorContext[T]): Receptionist.Command =
    Receptionist.find(expression._1, context.messageAdapter[Receptionist.Listing](expression._2))

}
