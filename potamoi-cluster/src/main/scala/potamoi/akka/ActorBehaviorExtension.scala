package potamoi.akka

import akka.actor.typed.{Behavior, SupervisorStrategy}
import akka.actor.typed.scaladsl.Behaviors

import scala.reflect.ClassTag

/**
 * Actor Behavior enhancement.
 */
object ActorBehaviorExtension:
  extension [U](behavior: Behavior[U]) {

    /**
     * Behaviors.supervise.onFailure
     */
    inline def onFailure[Thr <: Throwable](strategy: SupervisorStrategy)(implicit tag: ClassTag[Thr] = ClassTag(classOf[Throwable])): Behavior[U] = {
      Behaviors.supervise(behavior).onFailure[Thr](strategy)
    }

    /**
     * Execute the function before the behavior begins.
     */
    inline def beforeIt(func: => Unit): Behavior[U] = {
      func
      behavior
    }
  }
