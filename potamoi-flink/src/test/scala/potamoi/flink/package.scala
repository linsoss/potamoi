package potamoi

import potamoi.fs.{S3AccessStyle, S3Conf}

package object flink {

  val S3ConfDev = S3Conf(
    endpoint = "http://10.144.74.197:30255",
    bucket = "flink-dev",
    accessKey = "minio",
    secretKey = "minio123",
    accessStyle = S3AccessStyle.PathStyle,
    sslEnabled = false
  )

}
