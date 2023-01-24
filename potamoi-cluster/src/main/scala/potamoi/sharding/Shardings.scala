package potamoi.sharding

import com.devsisters.shardcake.*
import com.devsisters.shardcake.interfaces.*
import potamoi.kubernetes.K8sConf
import potamoi.sharding.store.StorageMemory
import zio.*

/**
 * Shardcake [[Sharding]] layer.
 */
//noinspection DuplicatedCode
object Shardings:

  lazy val test = memStore
  lazy val live = redisStore

  lazy val memStore: ZLayer[ShardingConf, Throwable, Sharding] = {
    val config        = ZLayer.service[ShardingConf].project(_.toConfig)
    val grpcConfig    = ZLayer.service[ShardingConf].project(_.toGrpcConfig)
    val grpcPods      = grpcConfig >>> GrpcPods.live
    val managerClient = config >>> ShardManagerClient.liveWithSttp
    val serializer    = KryoSerialization.live
    val storage       = managerClient >>> StorageMemory.live
    val sharding      = config ++ grpcPods ++ managerClient ++ storage ++ serializer >>> Sharding.live
    val grpcShardSvc  = config ++ sharding >>> GrpcShardingService.live
    sharding ++ grpcShardSvc
  }

  lazy val redisStore: ZLayer[ShardingConf with ShardRedisStoreConf, Throwable, Sharding] = {
    val config        = ZLayer.service[ShardingConf].project(_.toConfig)
    val grpcConfig    = ZLayer.service[ShardingConf].project(_.toGrpcConfig)
    val grpcPods      = grpcConfig >>> GrpcPods.live
    val managerClient = config >>> ShardManagerClient.liveWithSttp
    val serializer    = KryoSerialization.live
    val storage       = ShardcakeRedisStorage.live
    val sharding      = config ++ grpcPods ++ managerClient ++ storage ++ serializer >>> Sharding.live
    val grpcShardSvc  = config ++ sharding >>> GrpcShardingService.live
    sharding ++ grpcShardSvc
  }
