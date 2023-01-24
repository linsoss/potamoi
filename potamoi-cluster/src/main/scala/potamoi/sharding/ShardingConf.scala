package potamoi.sharding

import com.devsisters.shardcake.{Config, GrpcConfig}
import potamoi.codecs.scalaDurationJsonCodec
import potamoi.common.HoconConfig
import potamoi.times.given
import sttp.client3.UriContext
import zio.{ULayer, ZIO, ZLayer}
import zio.config.magnolia.{descriptor, name}
import zio.config.read

import scala.concurrent.duration.{Duration, DurationInt}

/**
 * Shardcake sharding client configuration.
 * see: [[com.devsisters.shardcake.Config]]
 */

object ShardingConf:

  val live: ZLayer[com.typesafe.config.Config, Throwable, ShardingConf] = ZLayer {
    for {
      source <- HoconConfig.hoconSource("potamoi.sharding")
      config <- read(descriptor[ShardingConf].from(source))
    } yield config
  }

  val test: ULayer[ShardingConf] = ZLayer.succeed(ShardingConf(simulateRemotePods = true))

case class ShardingConf(
    @name("number-of-shards") numberOfShards: Int = 300,
    @name("self-host") selfHost: String = "localhost",
    @name("self-port") selfPort: Int = 3400,
    @name("server-host") serverHost: String = "localhost",
    @name("server-port") serverPort: Int = 3300,
    @name("server-version") serverVersion: String = "1.0.0",
    @name("entity-max-idle-time") entityMaxIdleTime: Duration = 1.minutes,
    @name("entity-termination-timeout") entityTerminationTimeout: Duration = 3.seconds,
    @name("send-timeout") sendTimeout: Duration = 10.seconds,
    @name("refresh-assignments-retry-interval") refreshAssignmentsRetryInterval: Duration = 5.seconds,
    @name("unhealthy-pod-report-interval") unhealthyPodReportInterval: Duration = 5.seconds,
    @name("simulate-remote-pods") simulateRemotePods: Boolean = false,
    @name("grpc-max-inbound") grpcMaxInbound: Int = 32 * 1024 * 1024):

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
