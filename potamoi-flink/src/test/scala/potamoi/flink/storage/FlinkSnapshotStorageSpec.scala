package potamoi.flink.storage

import potamoi.flink.model.Fcid
import zio.test.*
import zio.test.Assertion.*
import zio.{Scope, ZIO, ZLayer}

object FlinkSnapshotStorageSpec extends ZIOSpecDefault:
  
  val spec = suite("FlinkSnapshotStorage")(
    suite("storage interface")(
      test("call api normally") {
        for {
          stg <- ZIO.service[FlinkSnapshotStorage]
          _   <- stg.trackedList.put(Fcid("a", "b"))
          r   <- stg.trackedList.exists(Fcid("a", "b"))
        } yield assertTrue(r)
      },
      test("convert to query normally") {
        for {
          stg <- ZIO.service[FlinkSnapshotStorage]
          _   <- stg.trackedList.put(Fcid("a", "b"))
          query = stg.narrowQuery
          r <- query.trackedList.exists(Fcid("a", "b"))
        } yield assertTrue(r)
      }
    ).provide(FlinkSnapshotStorage.memory),
    test("query interface layer") {
      for {
        stg   <- ZIO.service[FlinkSnapshotStorage]
        _     <- stg.trackedList.put(Fcid("a", "b"))
        query <- ZIO.service[FlinkSnapshotQuery]
        r     <- query.trackedList.exists(Fcid("a", "b"))
      } yield assertTrue(r)
    }.provide(FlinkSnapshotStorage.memory, FlinkSnapshotQuery.live)
  )
