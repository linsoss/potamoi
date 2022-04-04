package com.github.potamois.potamoi.akka.toolkit

import akka.actor.typed.receptionist.Receptionist.Listing
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.{ActorRef, Settings}

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * Actor implicit conversion function, class.
 *
 * @author Al-assad
 */
object ActorImplicit {

  def settings(implicit context: ActorContext[_]): Settings = context.system.settings

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

  def settings(implicit context: ActorContext[_]): Settings = context.system.settings

  def receptionist(implicit context: ActorContext[T]): ActorRef[Receptionist.Command] = context.system.receptionist

  def messageAdapter[U: ClassTag](f: U => T)(implicit context: ActorContext[T]): ActorRef[U] = context.messageAdapter[U](f)

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
