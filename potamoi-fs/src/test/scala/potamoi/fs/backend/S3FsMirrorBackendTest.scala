package potamoi.fs.backend

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{BeforeAndAfterAll, Ignore}
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

@Ignore
class S3FsMirrorBackendTest extends AnyWordSpec with BeforeAndAfterAll:

  val layer = S3FsBackendConfDev.asLayer >>> S3FsMirrorBackend.live

  def testing[E, A](f: S3FsMirrorBackend => IO[E, A]) =
    ZIO
      .service[S3FsMirrorBackend]
      .flatMap { b => f(b) }
      .provideLayer(layer)
      .provideLayer(PotaLogger.layer(level = LogsLevel.Debug))
      .run
      .exitCode

  "S3FsMirrorBackend download" in testing { backend =>
    backend
      .download("pota://flink-connector-jdbc-1.15.2.jar", "spec-test/flink-connector-jdbc-1.15.2.jar")
      .debugPretty
  }

  "S3FsMirrorBackend download2" in testing { backend =>
    backend
      .upload(File("spec-test/flink-connector-jdbc-1.15.2.jar"), "pota://flink-connector-jdbc-1.15.2@re.jar")
      .debugPretty
  }

  "S3FsMirrorBackend download3" in testing { backend =>
    backend
      .download("pota://flink-connector-jdbc-1.15.2@re.jar", "spec-test/flink-connector-jdbc-1.15.2@re.jar")
      .debugPretty *>
    backend
      .download("pota://flink-connector-jdbc-1.15.2@re.jar", "spec-test/flink-connector-jdbc-1.15.2@re.jar")
      .debugPretty
  }

  "S3FsMirrorBackend remove" in testing { backend =>
    backend
      .remove("pota://flink-connector-jdbc-1.15.2@re.jar")
      .debugPretty
  }

  "S3FsMirrorBackend remove" in testing { backend =>
    backend.exist("pota://flink-connector-jdbc-1.15.2@re.jar").debug *>
    backend.exist("pota://flink-connector-jdbc-1.15.2.jar").debug
  }

  // override protected def afterAll(): Unit = lfs.rm("spec-test").run
