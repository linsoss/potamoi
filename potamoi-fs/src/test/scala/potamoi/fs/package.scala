package potamoi

import potamoi.fs.refactor.{FileServerConf, LocalFsBackendConf, S3FsBackendConf}
import potamoi.fs.refactor.S3AccessStyle.PathStyle

package object fs:

  val S3FsBackendConfDev = S3FsBackendConf(
    endpoint = "http://10.144.74.197:30255",
    bucket = "flink-dev",
    accessKey = "minio",
    secretKey = "minio123",
    accessStyle = PathStyle
  )

  val LocalFsBackendConfDev = LocalFsBackendConf().resolve("var/potamoi")

  val FileServerConfDev = FileServerConf(host = "10.144.108.28")
