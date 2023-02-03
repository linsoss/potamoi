package potamoi.akka

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.actor.typed.scaladsl.Behaviors
import potamoi.logger.PotaLogger
import potamoi.{HoconConfig, KryoSerializable}
import potamoi.akka.actors.*
import potamoi.akka.Worker.*
import potamoi.zios.debugPretty
import zio.{Duration, Scope, ZIO, ZIOAppDefault, ZLayer}

object ReceptionistTestApp extends ZIOAppDefault {

  override val bootstrap = PotaLogger.default

  val effect = for {
    matrix  <- ZIO.service[AkkaMatrix]
    worker1 <- matrix.spawn("worker-1", Worker("worker-1"))
    worker2 <- matrix.spawn("worker-2", Worker("worker-2"))
    _       <- worker1 !> Touch("hello")
    _       <- worker2 !> Touch("hello")
    _       <- matrix.findReceptionist(WorkerService).repeatUntil(_.nonEmpty).debugPretty
    _       <- matrix.stop(worker1)
    _       <- matrix.findReceptionist(WorkerService).repeatUntil(_.size == 1).debugPretty
    _       <- ZIO.never
  } yield ()

  val run = effect
    .provide(
      Scope.default,
      HoconConfig.empty,
      AkkaConf.local(),
      AkkaMatrix.live
    )
}

object Worker {

  val WorkerService = ServiceKey[Event]("WorkerService")

  sealed trait Event                extends KryoSerializable
  case class Touch(message: String) extends Event

  def apply(name: String): Behavior[Event] = Behaviors.setup { ctx =>
    ctx.system.receptionist ! Receptionist.Register(WorkerService, ctx.self)
    Behaviors.receiveMessage { case Touch(msg) =>
      ctx.log.info(s"[$name] be touch: $msg")
      Behaviors.same
    }
  }
}
