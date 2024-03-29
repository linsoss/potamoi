package potamoi.akka

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.Behavior
import potamoi.zios.*
import potamoi.HoconConfig
import potamoi.akka.TickBot.*
import potamoi.akka.actors.*
import potamoi.akka.zios.*
import potamoi.logger.{LogConf, PotaLogger}
import potamoi.logger.PotaLogger.akkaSourceMdc
import zio.{Clock, Schedule, *}
import zio.stream.ZStream

import java.util.concurrent.TimeUnit

object ActorZIOInterpTest extends ZIOAppDefault {

  override val bootstrap = PotaLogger.default

  val effect =
    for {
      matrix <- ZIO.service[AkkaMatrix]
      bot    <- matrix.spawnAnonymous(TickBot())

      _ <- bot !> Start
      _ <- ZIO.sleep(10.seconds)
      _ <- bot !> Start
    } yield ()

  val run = effect.provide(
    Scope.default,
    HoconConfig.empty,
    AkkaConf.local(),
    AkkaMatrix.live
  )
}

object TickBot {

  sealed trait Event
  case object Start extends Event
  case object Stop  extends Event

  def apply(): Behavior[Event] = Behaviors.setup { ctx =>

    var proc: Option[CancelableFuture[Unit]] = None
    ctx.log.info("TickBot started.")

    given LogConf = LogConf()
    given ActorContext[_] = ctx

    Behaviors.receiveMessage {
      case Start =>
        val effect =
          ZStream
            .fromZIO(Clock.currentTime(TimeUnit.SECONDS))
            .repeat(Schedule.spaced(1.seconds))
            .tap(t => ZIO.logInfo(s"tick: $t"))
            .runDrain
        @@ ZIOAspect.annotated(akkaSourceMdc -> ctx.self.path.toString)
        proc = Some(effect.runAsync)
        Behaviors.same
      case Stop  =>
        proc.map(_.cancel())
        Behaviors.same
    }
  }
}
