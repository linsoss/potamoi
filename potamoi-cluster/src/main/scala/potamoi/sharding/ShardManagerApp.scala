package potamoi.sharding

import com.devsisters.shardcake.*
import com.devsisters.shardcake.interfaces.*
import potamoi.logger.PotaLogger
import potamoi.zios.asLayer
import zio.*
import zio.http.Client
import zio.http.model.Status

/**
 * Shardcake manager app.
 * todo read config from hocon and env.
 */
object ShardManagerApp extends ZIOAppDefault:
  override val bootstrap = PotaLogger.default
  def run                = Server.run.provide(ShardManagerConf().asLayer, ShardManagers.test)


