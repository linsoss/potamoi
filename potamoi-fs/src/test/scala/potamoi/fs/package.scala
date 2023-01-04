package potamoi

import potamoi.fs.refactor.S3FsBackendConf
import potamoi.fs.refactor.S3AccessStyle.PathStyle

package object fs:

  val S3FsBackendConfTest = S3FsBackendConf(
    endpoint = "http://10.144.74.197:30255",
    bucket = "flink-dev",
    accessKey = "minio",
    secretKey = "minio123",
    accessStyle = PathStyle
  )
