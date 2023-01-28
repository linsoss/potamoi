package potamoi.akka

import akka.actor.typed.Behavior
import potamoi.logger.PotaLogger
import zio.{RIO, Scope, Task, ZIO, ZIOAppDefault, ZLayer}
import potamoi.akka.actors.*
import potamoi.HoconConfig

import potamoi.zios.debugPretty

object LWWMapDDataTest extends ZIOAppDefault {

  override val bootstrap = PotaLogger.default

  object DemoMapCache extends LWWMapDData[Int, String]("demo-map-cache"):
    def apply(): Behavior[DemoMapCache.Req] = behavior(DDataConf.default)
  
  import DemoMapCache.op.*

  val effect: RIO[ActorCradle, Unit] =
    for {
      cradle           <- ZIO.service[ActorCradle]
      cache            <- cradle.spawnAnonymous(DemoMapCache())
      given ActorCradle = cradle
      _                <- cache.size().debugPretty
      _                <- cache.put(1, "one")
      _                <- cache.put(2, "two")
      _                <- cache.puts(List(3 -> "three", 4 -> "four"))
      _                <- cache.get(1).debugPretty
      _                <- cache.size().debugPretty
      _                <- cache.remove(1)
      _                <- cache.listAll().debugPretty
      _                <- cache.update(2, _ => "two-two")
      _                <- cache.get(2).debugPretty
      _                <- cache.upsert(10, "ten", _ => "ten-replace")
      _                <- cache.get(10).debugPretty
      _                <- cache.upsert(10, "ten", _ => "ten-replace")
      _                <- cache.get(10).debugPretty
    } yield ()

  val run = effect.provide(
    Scope.default,
    HoconConfig.empty,
    AkkaConf.local(),
    ActorCradle.live
  )
}
