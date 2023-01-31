package potamoi

import potamoi.akka.{ActorBehaviorExtension, ActorOpExtension, ActorZIOExtension}

package object akka:

  val actors    = ActorOpExtension
  val behaviors = ActorBehaviorExtension
  val zios      = ActorZIOExtension
