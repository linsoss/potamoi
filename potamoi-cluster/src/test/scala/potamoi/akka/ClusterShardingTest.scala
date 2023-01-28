package potamoi.akka

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import potamoi.akka.{Bot, BotAssign}
import potamoi.akka.Bot.*
import potamoi.akka.BotAssign.*
import potamoi.logger.PotaLogger
import potamoi.HoconConfig
import potamoi.akka.actors.*
import zio.{Duration, Scope, ZIO, ZIOAppDefault}

object BotApp1 extends ZIOAppDefault {

  override val bootstrap = PotaLogger.default

  val effect = for {
    cradle           <- ZIO.service[ActorCradle]
    given ActorCradle = cradle
    ticker           <- cradle.spawn("bot-assign", BotAssign())
    _                <- ticker !> Proxy("t-1", Bot.Touch("hello world"))
    reply            <- ticker.?>[String](res => Proxy("t-1", Bot.Echo("hello world", res)))
    _                <- ZIO.logInfo(reply)
    _                <- ZIO.never
  } yield ()

  val run = effect.provide(
    Scope.default,
    HoconConfig.empty,
    AkkaConf.localCluster(3301, List(3301, 3302)),
    ActorCradle.live
  )

}

object BotApp2 extends ZIOAppDefault {

  override val bootstrap = PotaLogger.default

  val effect = for {
    cradle <- ZIO.service[ActorCradle]
    _      <- cradle.spawn("bot-assign", BotAssign())
    _      <- ZIO.never
  } yield ()

  val run = effect
    .provide(
      Scope.default,
      HoconConfig.empty,
      AkkaConf.localCluster(3302, List(3301, 3302), List("ticker")),
      ActorCradle.live
    )
}

object Bot {

  sealed trait Event                                        extends KryoSerializable
  case class Touch(message: String)                         extends Event
  case class Echo(message: String, reply: ActorRef[String]) extends Event

  def apply(entityId: String): Behavior[Event] = Behaviors.setup { ctx =>
    Behaviors.receiveMessage {
      case Touch(msg) =>
        ctx.log.info(s"[$entityId] be touch: $msg")
        Behaviors.same

      case Echo(msg, reply) =>
        reply ! s"from $entityId: $msg"
        Behaviors.same
    }
  }
}

object BotAssign {

  case class Proxy(id: String, event: Bot.Event)

  def apply(): Behavior[Proxy] = Behaviors.setup { ctx =>
    val sharding = ClusterSharding(ctx.system)
    val typeKey  = EntityTypeKey[Bot.Event]("Bots")
    val region   = sharding.init {
      Entity(typeKey)(createBehavior = entityContext => Bot(entityContext.entityId))
        .withRole("ticker")
    }
    Behaviors.receiveMessage { case Proxy(id, event) =>
      region ! ShardingEnvelope(id, event)
      Behaviors.same
    }
  }
}
