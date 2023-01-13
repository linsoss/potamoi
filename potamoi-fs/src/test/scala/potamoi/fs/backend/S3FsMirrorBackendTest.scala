package potamoi.fs.backend

import potamoi.PotaErr.logErrorCausePretty
import potamoi.fs.refactor.{lfs, S3AccessStyle, S3FsBackendConf}
import potamoi.fs.refactor.backend.{S3FsBackend, S3FsMirrorBackend}
import potamoi.fs.S3FsBackendConfDev
import potamoi.logger.{LogsLevel, PotaLogger}
import potamoi.zios.*
import potamoi.PotaErr
import zio.{IO, ZIO, ZLayer}
import zio.ZIO.{logErrorCause, logLevel}

import java.io.File

object S3FsMirrorBackendTest:

  val layer = S3FsBackendConfDev.asLayer >>> S3FsMirrorBackend.live

  def testing[E, A](f: S3FsMirrorBackend => IO[E, A]) =
    zioRun {
      ZIO
        .service[S3FsMirrorBackend]
        .flatMap { b => f(b) }
        .provideLayer(layer)
        .provideLayer(PotaLogger.layer(level = LogsLevel.Debug))
    }.exitCode

  @main def testDownload2 = testing { backend =>
    backend
      .download("pota://flink-connector-jdbc-1.15.2.jar", "spec-test/flink-connector-jdbc-1.15.2.jar")
      .debugPretty
  }

  @main def testUpload2 = testing { backend =>
    backend
      .upload(File("spec-test/flink-connector-jdbc-1.15.2.jar"), "pota://flink-connector-jdbc-1.15.2@re.jar")
      .debugPretty
  }

  @main def testDownload3 = testing { backend =>
    backend
      .download("pota://flink-connector-jdbc-1.15.2@re.jar", "spec-test/flink-connector-jdbc-1.15.2@re.jar")
      .debugPretty *>
    backend
      .download("pota://flink-connector-jdbc-1.15.2@re.jar", "spec-test/flink-connector-jdbc-1.15.2@re.jar")
      .debugPretty
  }

  @main def testRemove2 = testing { backend =>
    backend
      .remove("pota://flink-connector-jdbc-1.15.2@re.jar")
      .debugPretty
  }

  @main def testExists2 = testing { backend =>
    backend.exist("pota://flink-connector-jdbc-1.15.2@re.jar").debug *>
    backend.exist("pota://flink-connector-jdbc-1.15.2.jar").debug
  }

  @main def cleanTmpDir2 = lfs.rm("spec-test").run
