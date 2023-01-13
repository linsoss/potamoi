package potamoi.fs.backend

import potamoi.fs.refactor.backend.{LocalFsBackend, S3FsBackend}
import potamoi.fs.refactor.{lfs, S3AccessStyle, S3FsBackendConf}
import potamoi.fs.{LocalFsBackendConfDev, S3FsBackendConfDev}
import zio.{IO, ZIO, ZLayer}
import potamoi.zios.*
import potamoi.errs.*
import potamoi.PotaErr.logErrorCausePretty
import potamoi.PotaErr

import java.io.File

object LocalFsBackendTest:

  val layer = LocalFsBackendConfDev.asLayer >>> LocalFsBackend.live

  def testing[E, A](f: LocalFsBackend => IO[E, A]) =
    zioRun {
      ZIO
        .service[LocalFsBackend]
        .flatMap { b => f(b) }
        .provide(layer)
    }.exitCode

  @main def testLfsDownload = testing { backend =>
    backend
      .download("pota://README.md", "spec-test/README.md")
      .debugPretty
  }

  @main def testLfsUpload = testing { backend =>
    backend
      .upload(File("spec-test/README.md"), "pota://README2.md")
      .debugPretty
  }

  @main def testLfsRemove = testing { backend =>
    backend
      .remove("pota://README2.md")
      .debugPretty
  }

  @main def testLfsExists = testing { backend =>
    backend.exist("pota://README2.md").debugPretty *>
    backend.exist("pota://README.md").debugPretty
  }

  @main def cleanLfsTmpDir = lfs.rm("spec-test").run
