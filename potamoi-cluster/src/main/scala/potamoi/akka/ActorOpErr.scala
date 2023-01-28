package potamoi.akka

import potamoi.PotaErr

sealed trait ActorOpErr extends PotaErr

object ActorOpErr:

  case class AskFailure(actorPath: String, cause: Throwable) extends ActorOpErr
