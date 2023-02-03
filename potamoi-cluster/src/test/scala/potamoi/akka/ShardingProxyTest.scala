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
    matrix           <- ZIO.service[AkkaMatrix]
    given AkkaMatrix = matrix
    proxy            <- matrix.spawn("bot-proxy", BotProxy())
    _                <- proxy("b1").tellZIO(Bot.Touch("hello"))
    proxy2           <- matrix.spawn("bot-proxy2", BotProxy())
    _                <- proxy2("b2").tellZIO(Bot.Touch("hello"))
    _                <- ZIO.never
  } yield ()

  val run = effect
    .provide(
      Scope.default,
      HoconConfig.empty,
      AkkaConf.local(),
      AkkaMatrix.live
    )
}

object BotProxy extends ShardingProxy[String, Bot.Event] {
  def apply(): Behavior[Req] = behavior(
    entityKey = EntityTypeKey[Bot.Event]("bot"),
    marshallKey = identity,
    unmarshallKey = identity,
    createBehavior = entityId => Bot(entityId))
}

object BotProxy2 extends ShardingProxy[String, Bot.Event] {
  def apply(): Behavior[Req] = behavior(
    entityKey = EntityTypeKey[Bot.Event]("bot2"),
    marshallKey = identity,
    unmarshallKey = identity,
    createBehavior = entityId => Bot(entityId)
  )
}
