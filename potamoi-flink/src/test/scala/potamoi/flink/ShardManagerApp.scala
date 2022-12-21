package potamoi.flink

import com.devsisters.shardcake.Server
import potamoi.logger.PotaLogger
import potamoi.sharding.{ShardManagerConf, ShardManagers}
import potamoi.zios.*
import zio.ZIOAppDefault

object ShardManagerApp extends ZIOAppDefault:
  override val bootstrap = PotaLogger.default
  def run                = Server.run.provide(ShardManagerConf.test.asLayer, ShardManagers.test)
