package potamoi.fs.backend

import potamoi.fs.refactor.backend.S3FsBackend
import potamoi.fs.refactor.{lfs, S3AccessStyle, S3FsBackendConf}
import potamoi.fs.S3FsBackendConfDev
import zio.{IO, ZIO, ZLayer}
import potamoi.zios.*
import potamoi.PotaErr.logErrorCausePretty
import potamoi.PotaErr.logErrorCausePretty
import potamoi.PotaErr

import java.io.File

object S3FsBackendTest:

  val layer = S3FsBackendConfDev.asLayer >>> S3FsBackend.live

  def testing[E, A](f: S3FsBackend => IO[E, A]) =
    zioRun {
      ZIO
        .service[S3FsBackend]
        .flatMap { b => f(b) }
        .provide(layer)
    }.exitCode

  @main def testDownload = testing { backend =>
    backend
      .download("pota://flink-connector-jdbc-1.15.2.jar", "spec-test/flink-connector-jdbc-1.15.2.jar")
      .debugPretty
  }

  @main def testUpload = testing { backend =>
    backend
      .upload(File("spec-test/flink-connector-jdbc-1.15.2.jar"), "pota://flink-connector-jdbc-1.15.2@re.jar")
      .debugPretty
  }

  @main def testRemove = testing { backend =>
    backend
      .remove("pota://flink-connector-jdbc-1.15.2@re.jar")
      .debugPretty
  }

  @main def testExists = testing { backend =>
    backend.exist("pota://flink-connector-jdbc-1.15.2@re.jar").debugPretty *>
    backend.exist("pota://flink-connector-jdbc-1.15.2.jar").debugPretty
  }

  @main def cleanTmpDir = lfs.rm("spec-test").run
