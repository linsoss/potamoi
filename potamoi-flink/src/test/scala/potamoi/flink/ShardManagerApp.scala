package potamoi.flink

import com.devsisters.shardcake.Server
import potamoi.logger.PotaLogger
import potamoi.sharding.{ShardManagerConf, ShardManagers}
import zio.ZIOAppDefault
import potamoi.zios.*

object ShardManagerApp extends ZIOAppDefault:
  override val bootstrap = PotaLogger.default
  def run                = Server.run.provide(ShardManagerConf.test.asLayer, ShardManagers.test)
