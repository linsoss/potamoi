package potamoi.fs.backend

import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Ignore}
import org.scalatest.wordspec.{AnyWordSpec, AnyWordSpecLike}
import potamoi.fs.refactor.{lfs, LocalFsBackendConf, S3AccessStyle, S3FsBackendConf}
import potamoi.fs.refactor.backend.{LocalFsBackend, S3FsBackend}
import potamoi.zios.*
import potamoi.PotaErr
import potamoi.logger.PotaLogger
import potamoi.FsBackendConfDev.given
import zio.{IO, ZIO, ZLayer}

import java.io.File

@DoNotDiscover
class LocalFsBackendSpec extends AnyWordSpec with BeforeAndAfterAll:

  def testing[E, A](f: LocalFsBackend => IO[E, A]) =
    ZIO
      .service[LocalFsBackend]
      .flatMap { b => f(b) }
      .provide(LocalFsBackendConf.test >>> LocalFsBackend.live)
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
