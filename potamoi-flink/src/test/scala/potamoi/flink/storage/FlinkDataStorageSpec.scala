package potamoi.flink.storage

import potamoi.flink.model.Fcid
import zio.{Scope, ZIO, ZLayer}
import zio.test.*
import zio.test.Assertion.*

object FlinkDataStorageSpec extends ZIOSpecDefault:

  val spec = suite("FlinkSnapshotStorage")(
    suite("storage interface")(
      test("call api normally") {
        for {
          stg <- ZIO.service[FlinkDataStorage]
          _   <- stg.trackedList.put(Fcid("a", "b"))
          r   <- stg.trackedList.exists(Fcid("a", "b"))
        } yield assertTrue(r)
      }
    ).provide(FlinkDataStorage.memory),
  )