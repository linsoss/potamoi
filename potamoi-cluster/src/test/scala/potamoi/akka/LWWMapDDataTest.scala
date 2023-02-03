package potamoi.akka

import akka.actor.typed.Behavior
import potamoi.akka.actors.*
import potamoi.logger.PotaLogger
import potamoi.HoconConfig
import potamoi.zios.debugPretty
import zio.{RIO, Scope, Task, ZIO, ZIOAppDefault, ZLayer}
import potamoi.akka.LWWMapDDataTest.DemoMapCache.ops

object LWWMapDDataTest extends ZIOAppDefault {

  override val bootstrap = PotaLogger.default

  object DemoMapCache extends LWWMapDData[Int, String]("demo-map-cache"):
    def apply(): Behavior[DemoMapCache.Req] = behavior(DDataConf.default)

  val effect: RIO[AkkaMatrix, Unit] =
    for {
      matrix           <- ZIO.service[AkkaMatrix]
      cache            <- matrix.spawnAnonymous(DemoMapCache())
      given AkkaMatrix = matrix
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
      _ <- ZIO.never
    } yield ()

  val run = effect.debugPretty.provide(
    Scope.default,
    HoconConfig.empty,
    AkkaConf.local(),
    AkkaMatrix.live
  )
}
