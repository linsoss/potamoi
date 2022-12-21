package potamoi.sharding

import com.devsisters.shardcake.{Config, GrpcConfig, ManagerConfig}
import potamoi.times.given
import sttp.client3.UriContext
import zio.json.{DeriveJsonCodec, JsonCodec}

import scala.concurrent.duration.{Duration, DurationInt}

/**
 * Shardcake manager configuration.
 * see: [[com.devsisters.shardcake.ManagerConfig]]
 */
case class ShardManagerConf(
    numberOfShards: Int = 300,
    port: Int = 3300,
    rebalanceInterval: Duration = 20.seconds,
    rebalanceRetryInterval: Duration = 10.seconds,
    pingTimeout: Duration = 3.seconds,
    persistRetryInterval: Duration = 3.seconds,
    persistRetryCount: Int = 100,
    rebalanceRate: Double = 2 / 100d,
    grpcMaxInbound: Int = 32 * 1024 * 1024):

  def toManagerConfig: ManagerConfig = ManagerConfig(
    apiPort = port,
    numberOfShards = numberOfShards,
    rebalanceInterval = rebalanceInterval,
    rebalanceRetryInterval = rebalanceRetryInterval,
    pingTimeout = pingTimeout,
    persistRetryInterval = persistRetryInterval,
    persistRetryCount = persistRetryCount,
    rebalanceRate = rebalanceRate
  )
  def toGrpcConfig: GrpcConfig = GrpcConfig(maxInboundMessageSize = grpcMaxInbound)

object ShardManagerConf:
  import potamoi.common.Codec.scalaDurationJsonCodec
  given JsonCodec[ShardManagerConf] = DeriveJsonCodec.gen[ShardManagerConf]
  val test                          = ShardManagerConf()

/**
 * Shardcake sharding client configuration.
 * see: [[com.devsisters.shardcake.Config]]
 */
case class ShardingConf(
    numberOfShards: Int = 300,
    selfHost: String = "localhost",
    selfPort: Int = 54321,
    serverHost: String = "localhost",
    serverPort: Int = 3300,
    serverVersion: String = "1.0.0",
    entityMaxIdleTime: Duration = 1.minutes,
    entityTerminationTimeout: Duration = 3.seconds,
    sendTimeout: Duration = 10.seconds,
    refreshAssignmentsRetryInterval: Duration = 5.seconds,
    unhealthyPodReportInterval: Duration = 5.seconds,
    simulateRemotePods: Boolean = false,
    grpcMaxInbound: Int = 32 * 1024 * 1024):

  def toConfig: Config = Config(
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
  import potamoi.common.Codec.scalaDurationJsonCodec
  given JsonCodec[ShardingConf] = DeriveJsonCodec.gen[ShardingConf]
  val test                      = ShardingConf()
