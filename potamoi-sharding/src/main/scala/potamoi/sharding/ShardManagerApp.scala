package potamoi.sharding

import com.devsisters.shardcake.*
import com.devsisters.shardcake.interfaces.*
import zio.*

object ShardManagerApp {

  def run: Task[Nothing] =
    Server.run.provide(
      ZLayer.succeed(ManagerConfig.default),
      ZLayer.succeed(GrpcConfig.default),
      PodsHealth.local,
      GrpcPods.live,
      Storage.memory,
      ShardManager.live
    )

}