package potamoi

import potamoi.common.Syntax.toPrettyString
import potamoi.fs.{S3AccessStyle, S3Conf}
import potamoi.kubernetes.K8sConf
import zio.{durationInt, IO, ZIO}
import zio.Schedule.spaced
import zio.stream.ZStream

package object flink:
  
  val S3ConfDev = S3Conf(
    endpoint = "http://10.144.74.197:30255",
    bucket = "flink-dev",
    accessKey = "minio",
    secretKey = "minio123",
    accessStyle = S3AccessStyle.PathStyle,
    sslEnabled = false
  )
