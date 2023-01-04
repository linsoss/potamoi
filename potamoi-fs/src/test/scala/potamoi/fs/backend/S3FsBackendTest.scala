package potamoi.fs.backend

import potamoi.fs.refactor.backend.S3FsBackend
import potamoi.fs.refactor.{lfs, S3AccessStyle, S3FsBackendConf}
import potamoi.fs.S3FsBackendConfTest
import zio.{IO, ZIO, ZLayer}
import potamoi.zios.*

import java.io.File

object S3FsBackendTest:

  val layer = S3FsBackendConfTest.asLayer >>> S3FsBackend.live

  def testing[A](f: S3FsBackend => IO[Throwable, A]) = zioRun {
    ZIO.service[S3FsBackend].flatMap { b => f(b) }.provide(layer)
  }

  @main def testDownload = testing { backend =>
    backend
      .download("pota://flink-connector-jdbc-1.15.2.jar", "spec-test/flink-connector-jdbc-1.15.2.jar")
      .debug
  }

  @main def testUpload = testing { backend =>
    backend
      .upload(File("spec-test/flink-connector-jdbc-1.15.2.jar"), "pota://flink-connector-jdbc-1.15.2@re.jar")
      .debug
  }

  @main def testRemove = testing { backend =>
    backend
      .remove("pota://flink-connector-jdbc-1.15.2@re.jar")
      .debug
  }

  @main def testExists = testing { backend =>
    backend.exist("pota://flink-connector-jdbc-1.15.2@re.jar").debug *>
    backend.exist("pota://flink-connector-jdbc-1.15.2.jar").debug
  }

  @main def cleanTmpDir = lfs.rm("spec-test").run
