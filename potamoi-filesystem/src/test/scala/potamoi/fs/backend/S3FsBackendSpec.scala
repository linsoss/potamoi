package potamoi.fs.backend

import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Ignore}
import org.scalatest.wordspec.AnyWordSpec
import potamoi.fs.refactor.{lfs, S3AccessStyle, S3FsBackendConf}
import potamoi.fs.refactor.backend.S3FsBackend
import potamoi.FsBackendConfDev.given
import potamoi.zios.*
import potamoi.PotaErr
import zio.{IO, ZIO, ZLayer}

import java.io.File

@DoNotDiscover
class S3FsBackendSpec extends AnyWordSpec with BeforeAndAfterAll:

  def testing[E, A](f: S3FsBackend => IO[E, A]) =
    ZIO
      .service[S3FsBackend]
      .flatMap { b => f(b) }
      .provide(S3FsBackendConf.test >>> S3FsBackend.live)
      .run
      .exitCode

  "S3FsBackend download" in testing { backend =>
    backend
      .download("pota://flink-connector-jdbc-1.15.2.jar", "spec-test/flink-connector-jdbc-1.15.2.jar")
      .debugPretty
  }

  "S3FsBackend upload" in testing { backend =>
    backend
      .upload(File("spec-test/flink-connector-jdbc-1.15.2.jar"), "pota://flink-connector-jdbc-1.15.2@re.jar")
      .debugPretty
  }

  "S3FsBackend remove" in testing { backend =>
    backend
      .remove("pota://flink-connector-jdbc-1.15.2@re.jar")
      .debugPretty
  }

  "S3FsBackend exists" in testing { backend =>
    backend.exist("pota://flink-connector-jdbc-1.15.2@re.jar").debugPretty *>
    backend.exist("pota://flink-connector-jdbc-1.15.2.jar").debugPretty
  }

  // override protected def afterAll(): Unit = lfs.rm("spec-test").run
