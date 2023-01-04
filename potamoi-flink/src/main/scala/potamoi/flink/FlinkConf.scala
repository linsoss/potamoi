package potamoi.flink

import com.softwaremill.quicklens.modify
import potamoi.common.Codec
import potamoi.common.Codec.scalaDurationJsonCodec
import potamoi.fs.PathTool
import potamoi.fs.refactor.paths
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
    @name("log-failed-deploy") logFailedDeployReason: Boolean = false,
    @name("tracking") tracking: FlinkTrackConf = FlinkTrackConf(),
    @name("proxy") reverseProxy: FlinkReverseProxyConf = FlinkReverseProxyConf()):

  def resolve(rootDataDir: String): FlinkConf =
    if localTmpDir.startsWith(rootDataDir) then this
    else copy(localTmpDir = s"$rootDataDir/${paths.rmFirstSlash(localTmpDir)}")

object FlinkConf:
  import FlinkRestEndpointTypes.given
  given JsonCodec[Duration]              = scalaDurationJsonCodec
  given JsonCodec[FlinkTrackConf]        = DeriveJsonCodec.gen[FlinkTrackConf]
  given JsonCodec[FlinkReverseProxyConf] = DeriveJsonCodec.gen[FlinkReverseProxyConf]
  given JsonCodec[FlinkConf]             = DeriveJsonCodec.gen[FlinkConf]

  val default = FlinkConf()
  val test = FlinkConf()
    .modify(_.tracking.tmdDetailPolling)
    .setTo(500.millis)
    .modify(_.tracking.jmMetricsPolling)
    .setTo(500.millis)
    .modify(_.tracking.tmMetricsPolling)
    .setTo(500.millis)
    .modify(_.tracking.jobMetricsPolling)
    .setTo(500.millis)
    .modify(_.tracking.k8sPodMetricsPolling)
    .setTo(500.millis)

/**
 * Flink cluster tracking config.
 */
case class FlinkTrackConf(
    @name("log-failed-reason") logTrackersFailedInfo: Boolean = false,
    @name("poll-parallelism") pollParallelism: Int = 16,
    @name("endpoint-cache-sync-interval") eptCacheSyncInterval: Duration = 30.seconds,
    @name("job-overview-poll-interval") jobOvPolling: Duration = 500.millis,
    @name("cluster-overview-poll-interval") clusterOvPolling: Duration = 500.millis,
    @name("tm-detail-poll-interval") tmdDetailPolling: Duration = 2.seconds,
    @name("jm-metrics-poll-interval") jmMetricsPolling: Duration = 5.seconds,
    @name("tm-metrics-poll-interval") tmMetricsPolling: Duration = 5.seconds,
    @name("job-metrics-poll-interval") jobMetricsPolling: Duration = 2.seconds,
    @name("k8s-pod-metrics-poll-interval") k8sPodMetricsPolling: Duration = 4.seconds,
    @name("savepoint-trigger-poll-interval") savepointTriggerPolling: Duration = 100.millis)

/**
 * Flink ui reversed proxy config.
 */
case class FlinkReverseProxyConf(
    @name("route-table-cache-size") routeTableCacheSize: Int = 1000,
    @name("route-table-cache-ttl") routeTableCacheTtl: Duration = 45.seconds)

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
