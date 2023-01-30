package potamoi.akka

import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import potamoi.logger.PotaLogger
import potamoi.HoconConfig
import potamoi.akka.BotProxy.ops
import potamoi.akka.BotProxy2.ops
import zio.{Scope, ZIO, ZIOAppDefault}

object ShardingProxyTest extends ZIOAppDefault {

  override val bootstrap = PotaLogger.default

  val effect = for {
    cradle           <- ZIO.service[ActorCradle]
    given ActorCradle = cradle
    proxy            <- cradle.spawn("bot-proxy", BotProxy())
    _                <- proxy("b1").tellZIO(Bot.Touch("hello"))
    proxy2           <- cradle.spawn("bot-proxy2", BotProxy())
    _                <- proxy2("b2").tellZIO(Bot.Touch("hello"))
    _                <- ZIO.never
  } yield ()

  val run = effect
    .provide(
      Scope.default,
      HoconConfig.empty,
      AkkaConf.local(),
      ActorCradle.live
    )
}

object BotProxy extends ShardingProxy[String, Bot.Event] {

  val entityKey     = EntityTypeKey[Bot.Event]("bot")
  val marshallKey   = identity
  val unmarshallKey = identity

  def apply(): Behavior[Req] = behavior(createBehavior = entityId => Bot(entityId))
}

object BotProxy2 extends ShardingProxy[String, Bot.Event] {

  val entityKey     = EntityTypeKey[Bot.Event]("bot2")
  val marshallKey   = identity
  val unmarshallKey = identity

  def apply(): Behavior[Req] = behavior(createBehavior = entityId => Bot(entityId))
}
