package potamoi

import potamoi.common.Syntax.toPrettyString
import potamoi.fs.{S3AccessStyle, S3Conf}
import potamoi.kubernetes.K8sConf

import zio.Schedule.spaced
import zio.{IO, ZIO, durationInt}

package object flink:

  extension [E, A](io: IO[E, A]) {
    def watch: IO[E, Unit]                       = io.debug.repeat(spaced(1.seconds)).unit
    def watchTag(tag: String): IO[E, Unit]       = io.debug(tag).repeat(spaced(1.seconds)).unit
    def watchPretty: IO[E, Unit]                 = io.map(toPrettyString(_)).debug.repeat(spaced(1.seconds)).unit
    def watchPrettyTag(tag: String): IO[E, Unit] = io.map(toPrettyString(_)).debug(tag).repeat(spaced(1.seconds)).unit
  }

  val FlinkConfTest = FlinkConf.test.resolve("var/potamoi")

  val K8sConfTest = K8sConf.default

  val S3ConfTest = S3Conf(
    endpoint = "http://hs.assad.site:30255",
    bucket = "flink-dev",
    accessKey = "minio",
    secretKey = "minio123",
    accessStyle = S3AccessStyle.PathStyle
  )
