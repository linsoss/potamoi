package potamoi.flink

import potamoi.common.Codec
import potamoi.common.Codec.scalaDurationJsonCodec
import potamoi.fs.PathTool
import zio.config.magnolia.name
import zio.json.{DeriveJsonCodec, JsonCodec, JsonDecoder, JsonEncoder}

import scala.concurrent.duration.{Duration, DurationInt}

/**
 * Flink module configuration.
 */
case class FlinkConf(
    @name("k8s-account") k8sAccount: String = "flink-opr",
    @name("mc-image") minioClientImage: String = "minio/mc:RELEASE.2022-10-12T18-12-50Z",
    @name("local-tmpdir") localTmpDir: String = "tmp/flink",
    @name("rest-endpoint-internal") restEndpointTypeInternal: FlinkRestEndpointType = FlinkRestEndpointType.ClusterIp,
    @name("tracking") tracking: FlinkTrackConf = FlinkTrackConf()):

  def resolve(localStgDir: String): FlinkConf = copy(localTmpDir = s"${localStgDir}/${PathTool.rmSlashPrefix(localTmpDir)}")

object FlinkConf:
  import FlinkRestEndpointTypes.given
  given JsonCodec[Duration]       = scalaDurationJsonCodec
  given JsonCodec[FlinkTrackConf] = DeriveJsonCodec.gen[FlinkTrackConf]
  given JsonCodec[FlinkConf]      = DeriveJsonCodec.gen[FlinkConf]

/**
 * Flink cluster tracking config.
 */
case class FlinkTrackConf(
    @name("job-overview-poll-interval") jobOvPolling: Duration = 500.millis,
    @name("cluster-overview-poll-interval") clusterOvPolling: Duration = 500.millis,
    @name("tm-detail-poll-interval") tmdDetailPolling: Duration = 2.seconds,
    @name("jm-metrics-poll-interval") jmMetricsPolling: Duration = 5.seconds,
    @name("tm-metrics-poll-interval") tmMetricsPolling: Duration = 5.seconds,
    @name("job-metrics-poll-interval") jobMetricsPolling: Duration = 2.seconds,
    @name("k8s-pod-metrics-poll-interval") k8sPodMetricsPolling: Duration = 4.seconds,
    @name("savepoint-trigger-poll-interval") savepointTriggerPolling: Duration = 100.millis)

/**
 * Flink rest api export type.
 */
enum FlinkRestEndpointType(val value: String):
  case SvcDns    extends FlinkRestEndpointType("svc-dns")
  case ClusterIp extends FlinkRestEndpointType("cluster-ip")

object FlinkRestEndpointTypes:
  given JsonCodec[FlinkRestEndpointType] = JsonCodec(
    JsonEncoder[String].contramap(_.value),
    JsonDecoder[String].mapOrFail(s => FlinkRestEndpointType.values.find(_.value == s).map(Right(_)).getOrElse(Left("not matching value")))
  )