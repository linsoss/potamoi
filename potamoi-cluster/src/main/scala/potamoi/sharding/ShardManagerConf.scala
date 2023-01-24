package potamoi.sharding

import com.devsisters.shardcake.{Config, GrpcConfig, ManagerConfig}
import potamoi.codecs.scalaDurationJsonCodec
import potamoi.common.HoconConfig
import potamoi.times.given
import sttp.client3.UriContext
import zio.{ULayer, ZLayer}
import zio.config.magnolia.{descriptor, name}
import zio.config.read

import scala.concurrent.duration.{Duration, DurationInt}

/**
 * Shardcake manager configuration.
 * see: [[com.devsisters.shardcake.ManagerConfig]]
 */
object ShardManagerConf:

  val live: ZLayer[com.typesafe.config.Config, Throwable, ShardManagerConf] = ZLayer {
    for {
      source <- HoconConfig.hoconSource("potamoi.shard-manager")
      config <- read(descriptor[ShardManagerConf].from(source))
    } yield config
  }

  val test: ULayer[ShardManagerConf] = ZLayer.succeed(ShardManagerConf())

case class ShardManagerConf(
    @name("number-of-shards") numberOfShards: Int = 300,
    @name("port") port: Int = 3300,
    @name("rebalance-retry-interval") rebalanceRetryInterval: Duration = 10.seconds,
    @name("ping-timeout") pingTimeout: Duration = 3.seconds,
    @name("persist-retry-interval") persistRetryInterval: Duration = 3.seconds,
    @name("persist-retry-count") persistRetryCount: Int = 100,
    @name("rebalance-rate") rebalanceRate: Double = 2 / 100d,
    @name("grpc-max-inbound") grpcMaxInbound: Int = 32 * 1024 * 1024):

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

  def toGrpcConfig: GrpcConfig = GrpcConfig(maxInboundMessageSize = grpcMaxInbound)
