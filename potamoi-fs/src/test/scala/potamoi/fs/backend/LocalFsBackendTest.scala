package potamoi.fs.backend

import potamoi.fs.refactor.backend.{LocalFsBackend, S3FsBackend}
import potamoi.fs.refactor.{lfs, S3AccessStyle, S3FsBackendConf}
import potamoi.fs.{LocalFsBackendConfTest, S3FsBackendConfTest}
import zio.{IO, ZIO, ZLayer}
import potamoi.zios.*
import potamoi.errs.*

import java.io.File

object LocalFsBackendTest:

  val layer = LocalFsBackendConfTest.asLayer >>> LocalFsBackend.live

  def testing[A](f: LocalFsBackend => IO[Throwable, A]) = zioRun {
    ZIO.service[LocalFsBackend].flatMap { b => f(b) }.provide(layer)
      .tapErrorCause(cause => ZIO.logErrorCause(cause.headMessage, cause.recurse))
  }

  @main def testLfsDownload = testing { backend =>
    backend
      .download("pota://README.md", "spec-test/README.md")
      .debug
  }

  @main def testLfsUpload = testing { backend =>
    backend
      .upload(File("spec-test/README.md"), "pota://README2.md")
      .debug
  }

  @main def testLfsRemove = testing { backend =>
    backend
      .remove("pota://README2.md")
      .debug
  }

  @main def testLfsExists = testing { backend =>
    backend.exist("pota://README2.md").debug *>
      backend.exist("pota://README.md").debug
  }

  @main def cleanLfsTmpDir = lfs.rm("spec-test").run
