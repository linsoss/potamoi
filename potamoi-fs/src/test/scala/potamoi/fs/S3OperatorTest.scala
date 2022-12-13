package potamoi.fs

import potamoi.common.MimeType
import potamoi.zios.*
import zio.ZLayer

object S3OperatorTest:

  val layer = ZLayer.succeed(
    S3Conf(
      endpoint = "http://10.144.74.197:30255",
      bucket = "flink-dev",
      accessKey = "minio",
      secretKey = "minio123",
      accessStyle = S3AccessStyle.PathStyle
    )) >>> S3Operator.live

  @main def testDownload = zioRun {
    S3Operator
      .download("s3://flink-dev/flink-connector-jdbc-1.15.2.jar", "spec-test/flink-connector-jdbc-1.15.2.jar")
      .provide(layer)
      .debug
  }

  @main def testUpload = zioRun {
    S3Operator
      .upload("spec-test/flink-connector-jdbc-1.15.2.jar", "s3://flink-dev/flink-connector-jdbc-1.15.2@re.jar", MimeType.jar)
      .provide(layer)
      .debug
  }

  @main def testRemove = zioRun {
    S3Operator
      .remove("s3://flink-dev/flink-connector-jdbc-1.15.2@re.jar")
      .provide(layer)
      .debug
  }

  @main def testExists = zioRun {
    S3Operator
      .exists("s3://flink-dev/flink-connector-jdbc-1.15.2@re.jar")
      .provide(layer)
      .debug *>
    S3Operator
      .exists("s3://flink-dev/flink-connector-jdbc-1.15.2.jar")
      .provide(layer)
      .debug
  }

  @main def cleanTmpDir = lfs.rm("spec-test").run
