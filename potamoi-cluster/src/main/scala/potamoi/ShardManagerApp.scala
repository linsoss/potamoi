package potamoi

import com.devsisters.shardcake.*
import com.devsisters.shardcake.interfaces.*
import potamoi.kubernetes.K8sConf
import potamoi.logger.{LogConf, PotaLogger}
import potamoi.sharding.{ShardcakeRedisStorage, ShardManagerConf, ShardManagers, ShardRedisStoreConf}
import potamoi.zios.asLayer
import zio.*
import zio.http.Client
import zio.http.model.Status

/**
 * Shardcake manager app.
 * Using redis as storage and k8s-api as health-checking.
 */
object ShardManagerApp extends ZIOAppDefault:

  override val bootstrap = LogConf.live >>> PotaLogger.live

  val run = (ZIO.logInfo("Shardcake manager launching...") *> Server.run)
    .provide(
      ShardManagerConf.live,
      ShardRedisStoreConf.live,
      K8sConf.live,
      ShardManagers.live
    )

/**
 * Local shardcake manager app for debugging.
 * Using memory as storage.
 */
object DebugShardManagerApp extends ZIOAppDefault:

  override val bootstrap = PotaLogger.default

  val run = (ZIO.logInfo("Shardcake manager launching...") *> Server.run)
    .provide(ShardManagerConf.test, ShardManagers.test)
