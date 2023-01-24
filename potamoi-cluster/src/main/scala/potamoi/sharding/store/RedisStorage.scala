package potamoi.sharding.store

import com.devsisters.shardcake.{RedisConfig, StorageRedis}
import com.devsisters.shardcake.StorageRedis.Redis
import com.devsisters.shardcake.interfaces.Storage
import com.typesafe.config.Config
import dev.profunktor.redis4cats.Redis
import dev.profunktor.redis4cats.connection.RedisClient
import dev.profunktor.redis4cats.data.RedisCodec
import dev.profunktor.redis4cats.effect.Log
import dev.profunktor.redis4cats.pubsub.PubSub
import potamoi.common.HoconConfig
import zio.{Task, ZEnvironment, ZIO, ZLayer}
import zio.config.magnolia.{descriptor, name}
import zio.config.read
import zio.interop.catz.*
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Shardcake redis storage layer.
 */
object ShardcakeRedisStorage:

  val live: ZLayer[ShardRedisStoreConf, Throwable, Storage] = {
    val redis     = ZLayer.service[ShardRedisStoreConf].flatMap(conf => shardcakeRedisLayer(conf.get))
    val redisConf = ZLayer.succeed(RedisConfig.default)
    redis ++ redisConf >>> StorageRedis.live
  }

  private def shardcakeRedisLayer(redisConf: ShardRedisStoreConf): ZLayer[Any, Throwable, Redis] =
    ZLayer.scopedEnvironment {
      implicit val runtime: zio.Runtime[Any] = zio.Runtime.default
      implicit val logger: Log[Task]         = new Log[Task] {
        override def debug(msg: => String): Task[Unit] = ZIO.logDebug(msg)
        override def info(msg: => String): Task[Unit]  = ZIO.logInfo(msg)
        override def error(msg: => String): Task[Unit] = ZIO.logError(msg)
      }
      (for {
        client   <- RedisClient[Task].from(redisConf.redisUri)
        commands <- Redis[Task].fromClient(client, RedisCodec.Utf8)
        pubSub   <- PubSub.mkPubSubConnection[Task, String, String](client, RedisCodec.Utf8)
      } yield ZEnvironment(commands, pubSub)).toScopedZIO
    }

/**
 * Shardcake redis storage config.
 */
case class ShardRedisStoreConf(@name("uri") redisUri: String)

object ShardRedisStoreConf:
  val live: ZLayer[Config, Throwable, ShardRedisStoreConf] = ZLayer {
    for {
      source <- HoconConfig.hoconSource("potamoi.redis")
      config <- read(descriptor[ShardRedisStoreConf].from(source))
    } yield config
  }
