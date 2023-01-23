package potamoi.sharding

import com.devsisters.shardcake.{Config, GrpcConfig, ManagerConfig}
import potamoi.codecs.scalaDurationJsonCodec
import potamoi.times.given
import sttp.client3.UriContext
import zio.config.magnolia.name
import zio.json.{DeriveJsonCodec, JsonCodec}

import scala.concurrent.duration.{Duration, DurationInt}

/**
 * Shardcake manager configuration.
 * see: [[com.devsisters.shardcake.ManagerConfig]]
 */
case class ShardManagerConf(
    @name("number-of-shards") numberOfShards: Int = 300,
    @name("port") port: Int = 3300,
    @name("rebalance-retry-interval") rebalanceRetryInterval: Duration = 10.seconds,
    @name("ping-timeout") pingTimeout: Duration = 3.seconds,
    @name("persist-retry-interval") persistRetryInterval: Duration = 3.seconds,
    @name("persist-retry-count") persistRetryCount: Int = 100,
    @name("rebalance-rate") rebalanceRate: Double = 2 / 100d,
    @name("grpc-max-inbound") grpcMaxInbound: Int = 32 * 1024 * 1024)
    derives JsonCodec:

  def toManagerConfig: ManagerConfig = ManagerConfig(
    apiPort = port,
    numberOfShards = numberOfShards,
    rebalanceInterval = zio.Duration.Infinity,
    rebalanceRetryInterval = rebalanceRetryInterval,
    pingTimeout = pingTimeout,
    persistRetryInterval = persistRetryInterval,
    persistRetryCount = persistRetryCount,
    rebalanceRate = rebalanceRate
  )
  def toGrpcConfig: GrpcConfig       = GrpcConfig(maxInboundMessageSize = grpcMaxInbound)

object ShardManagerConf:
  val test = ShardManagerConf()

/**
 * Shardcake sharding client configuration.
 * see: [[com.devsisters.shardcake.Config]]
 */
case class ShardingConf(
    @name("number-of-shards") numberOfShards: Int = 300,
    @name("self-host") selfHost: String = "localhost",
    @name("self-port") selfPort: Int = 54321,
    @name("server-host") serverHost: String = "localhost",
    @name("server-port") serverPort: Int = 3300,
    @name("server-version") serverVersion: String = "1.0.0",
    @name("entity-max-idle-time") entityMaxIdleTime: Duration = 1.minutes,
    @name("entity-termination-timeout") entityTerminationTimeout: Duration = 3.seconds,
    @name("send-timeout") sendTimeout: Duration = 10.seconds,
    @name("refresh-assignments-retry-interval") refreshAssignmentsRetryInterval: Duration = 5.seconds,
    @name("unhealthy-pod-report-interval") unhealthyPodReportInterval: Duration = 5.seconds,
    @name("simulate-remote-pods") simulateRemotePods: Boolean = false,
    @name("grpc-max-inbound") grpcMaxInbound: Int = 32 * 1024 * 1024)
    derives JsonCodec:

  def toConfig: Config         = Config(
    numberOfShards = numberOfShards,
    selfHost = selfHost,
    shardingPort = selfPort,
    shardManagerUri = uri"http://${serverHost}:${serverPort}/api/graphql",
    serverVersion = serverVersion,
    entityMaxIdleTime = entityMaxIdleTime,
    entityTerminationTimeout = entityTerminationTimeout,
    sendTimeout = sendTimeout,
    refreshAssignmentsRetryInterval = refreshAssignmentsRetryInterval,
    unhealthyPodReportInterval = unhealthyPodReportInterval,
    simulateRemotePods = simulateRemotePods
  )
  def toGrpcConfig: GrpcConfig = GrpcConfig(maxInboundMessageSize = grpcMaxInbound)

object ShardingConf:
  val test = ShardingConf(simulateRemotePods = true)
