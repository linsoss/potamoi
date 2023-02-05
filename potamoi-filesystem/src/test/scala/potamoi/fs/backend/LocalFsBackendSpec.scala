package potamoi.fs.backend

import org.scalatest.wordspec.{AnyWordSpec, AnyWordSpecLike}
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Ignore}
import potamoi.fs.{LocalFsBackendConfDev, S3FsBackendConfDev}
import potamoi.fs.refactor.{lfs, S3AccessStyle, S3FsBackendConf}
import potamoi.fs.refactor.backend.{LocalFsBackend, S3FsBackend}
import potamoi.zios.*
import potamoi.PotaErr
import potamoi.logger.PotaLogger
import zio.{IO, ZIO, ZLayer}

import java.io.File

@DoNotDiscover
class LocalFsBackendSpec extends AnyWordSpec with BeforeAndAfterAll:

  def testing[E, A](f: LocalFsBackend => IO[E, A]) =
    ZIO
      .service[LocalFsBackend]
      .flatMap { b => f(b) }
      .provide(LocalFsBackendConfDev.asLayer >>> LocalFsBackend.live)
      .provideLayer(PotaLogger.default)
      .run

  "lfs download" in testing { backend =>
    backend
      .download("pota://README.md", "spec-test/README.md")
      .debugPretty
  }

  "lfs upload" in testing { backend =>
    backend
      .upload(File("spec-test/README.md"), "pota://README2.md")
      .debugPretty
  }

  "lfs remove" in testing { backend =>
    backend
      .remove("pota://README2.md")
      .debugPretty
  }

  "lfs exists" in testing { backend =>
    backend.exist("pota://README2.md").debugPretty *>
    backend.exist("pota://README.md").debugPretty
  }

  // override protected def afterAll(): Unit = lfs.rm("spec-test").run
