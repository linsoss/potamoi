package potamoi

import potamoi.fs.{FsBackendConf, LocalFsBackendConf, S3FsBackendConf}
import potamoi.fs.S3AccessStyle.PathStyle
import zio.{ULayer, ZLayer}

object FsBackendConfDev:
  given Conversion[S3FsBackendConf.type, S3FsBackendConfDev.type]       = _ => S3FsBackendConfDev
  given Conversion[LocalFsBackendConf.type, LocalFsBackendConfDev.type] = _ => LocalFsBackendConfDev

object S3FsBackendConfDev:
  val test: ULayer[S3FsBackendConf] = ZLayer.succeed(
    S3FsBackendConf(
      endpoint = "http://10.144.74.197:30255",
      bucket = "flink-dev",
      accessKey = "minio",
      secretKey = "minio123",
      accessStyle = PathStyle,
      enableMirrorCache = true
    ).resolve(BaseConfDev.testValue.dataDir)
  )

object LocalFsBackendConfDev:
  val test: ULayer[LocalFsBackendConf] = ZLayer.succeed(
    LocalFsBackendConf()
      .resolve(BaseConfDev.testValue.dataDir)
  )
