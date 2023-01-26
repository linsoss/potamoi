package potamoi.flink

import com.softwaremill.quicklens.modify
import com.typesafe.config.Config
import potamoi.{codecs, BaseConf}
import potamoi.common.{Codec, HoconConfig}
import potamoi.common.Codec.scalaDurationJsonCodec
import potamoi.flink.FlinkRestEndpointTypes.given
import potamoi.fs.refactor.paths
import zio.json.{DeriveJsonCodec, JsonCodec, JsonDecoder, JsonEncoder}
import zio.config.magnolia.{descriptor, name}
import zio.config.read
import zio.{ZIO, ZLayer}

import scala.concurrent.duration.{Duration, DurationInt}

/**
 * Flink module configuration.
 */
case class FlinkConf(
    @name("k8s-account") k8sAccount: String = "flink-opr",
    @name("mc-image") minioClientImage: String = "minio/mc:RELEASE.2022-10-12T18-12-50Z",
    @name("local-tmpdir") localTmpDir: String = "tmp/flink",
    @name("rest-endpoint-internal") restEndpointTypeInternal: FlinkRestEndpointType = FlinkRestEndpointType.ClusterIP,
    @name("log-failed-deploy") logFailedDeployReason: Boolean = false,
    @name("tracking") tracking: FlinkTrackConf = FlinkTrackConf(),
    @name("proxy") reverseProxy: FlinkReverseProxyConf = FlinkReverseProxyConf(),
    @name("interact") sqlInteract: FlinkSqlInteractorConf = FlinkSqlInteractorConf()):

  lazy val localTmpDeployDir = s"${localTmpDir}/deploy"
  lazy val localTmpInterpDir = s"${localTmpDir}/interp"

  def resolve(rootDataDir: String): FlinkConf = {
    if localTmpDir.startsWith(rootDataDir) then this
    else copy(localTmpDir = s"$rootDataDir/${paths.rmFirstSlash(localTmpDir)}")
  }

object FlinkConf:

  val live: ZLayer[BaseConf with Config, Throwable, FlinkConf] = ZLayer {
    for {
      baseConf      <- ZIO.service[BaseConf]
      source        <- HoconConfig.hoconSource("potamoi.flink")
      config        <- read(descriptor[FlinkConf].from(source))
      resolvedConfig = config.resolve(baseConf.dataDir)
    } yield resolvedConfig
  }

  val test: ZLayer[BaseConf, Throwable, FlinkConf] = ZLayer {
    for {
      rootDataDir   <- ZIO.service[BaseConf].map(_.dataDir)
      config         = FlinkConf()
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
      resolvedConfig = config.resolve(rootDataDir)
    } yield resolvedConfig
  }

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
 * Flink sql interactor config.
 */
case class FlinkSqlInteractorConf(
    @name("max-idle-timeout") maxIdleTimeout: Duration = 30.minutes,
    @name("rpc-timeout") rpcTimeout: Duration = Duration.Inf,
    @name("stream-poll-interval") streamPollingInterval: Duration = 500.millis)

/**
 * Flink rest api export type.
 */
enum FlinkRestEndpointType:
  case SvcDns
  case ClusterIP

object FlinkRestEndpointTypes:
  given JsonCodec[FlinkRestEndpointType] = codecs.simpleEnumJsonCodec(FlinkRestEndpointType.values)
