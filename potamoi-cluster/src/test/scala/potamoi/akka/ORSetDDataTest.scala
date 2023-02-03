package potamoi.akka

import akka.actor.typed.Behavior
import potamoi.akka.actors.*
import potamoi.logger.PotaLogger
import potamoi.HoconConfig
import potamoi.zios.debugPretty
import zio.{RIO, Scope, Task, ZIO, ZIOAppDefault, ZLayer}

object ORSetDDataTest extends ZIOAppDefault {

  override val bootstrap = PotaLogger.default

  object DemoSetCache extends ORSetDData[String]("demo-set-cache"):
    def apply(): Behavior[DemoSetCache.Req] = behavior(DDataConf.default)

  import DemoSetCache.ops

  val effect: RIO[AkkaMatrix, Unit] =
    for {
      matrix           <- ZIO.service[AkkaMatrix]
      cache            <- matrix.spawnAnonymous(DemoSetCache())
      given AkkaMatrix = matrix
      _                <- cache.size().debugPretty
      _                <- cache.put("a")
      _                <- cache.put("a")
      _                <- cache.put("b")
      _                <- cache.puts(Set("c", "d", "e"))
      _                <- cache.size().debugPretty
      _                <- cache.list().debugPretty
      _                <- cache.contains("a").debugPretty
      _                <- cache.remove("a")
      _                <- cache.contains("a").debugPretty
      _                <- cache.list().debugPretty
    } yield ()

  val run = effect.provide(
    Scope.default,
    HoconConfig.empty,
    AkkaConf.local(),
    AkkaMatrix.live
  )
}
