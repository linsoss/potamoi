package potamoi.sharding

import com.devsisters.shardcake.*
import com.devsisters.shardcake.interfaces.*
import potamoi.kubernetes.K8sConf
import zio.*

/**
 * Shardcake [[ShardManager]] layer.
 */
//noinspection DuplicatedCode
object ShardManagers:

  lazy val test = memStgLocalPod
  lazy val live = redisStgK8sPod

  /**
   * storage: memory, pod-health: local
   */
  lazy val memStgLocalPod: ZLayer[ShardManagerConf, Throwable, ShardManager with ManagerConfig] = {
    val managerConfig = ZLayer.service[ShardManagerConf].project(_.toManagerConfig)
    val grpcConfig    = ZLayer.service[ShardManagerConf].project(_.toGrpcConfig)
    val grpcPods      = grpcConfig >>> GrpcPods.live
    val podHealth     = grpcPods >>> PodsHealth.local
    val storage       = Storage.memory
    managerConfig ++ storage ++ grpcPods ++ podHealth >>> ShardManager.live ++ managerConfig
  }

  /**
   * storage: redis, pod-health: local
   */
  lazy val redisStgLocalPod: ZLayer[ShardManagerConf with ShardRedisStgConf, Throwable, ShardManager with ManagerConfig] = {
    val managerConfig = ZLayer.service[ShardManagerConf].project(_.toManagerConfig)
    val grpcConfig    = ZLayer.service[ShardManagerConf].project(_.toGrpcConfig)
    val grpcPods      = grpcConfig >>> GrpcPods.live
    val podHealth     = grpcPods >>> PodsHealth.local
    val storage       = ShardcakeRedisStorage.live
    managerConfig ++ storage ++ grpcPods ++ podHealth >>> ShardManager.live ++ managerConfig
  }

  /**
   * storage: redis, pod-health: k8s-api
   */
  lazy val redisStgK8sPod: ZLayer[ShardManagerConf with ShardRedisStgConf with K8sConf, Throwable, ShardManager with ManagerConfig] = {
    val managerConfig = ZLayer.service[ShardManagerConf].project(_.toManagerConfig)
    val grpcConfig    = ZLayer.service[ShardManagerConf].project(_.toGrpcConfig)
    val grpcPods      = grpcConfig >>> GrpcPods.live
    val podHealth     = ShardcakeK8sPodsHealth.live
    val storage       = ShardcakeRedisStorage.live
    managerConfig ++ storage ++ grpcPods ++ podHealth >>> ShardManager.live ++ managerConfig
  }

/**
 * Shardcake [[Sharding]] layer.
 */
//noinspection DuplicatedCode
object Shardings:

  lazy val test = memStg
  lazy val live = redisStg

  lazy val memStg: ZLayer[ShardingConf, Throwable, Sharding] = {
    val config        = ZLayer.service[ShardingConf].project(_.toConfig)
    val grpcConfig    = ZLayer.service[ShardingConf].project(_.toGrpcConfig)
    val grpcPods      = grpcConfig >>> GrpcPods.live
    val managerClient = config >>> ShardManagerClient.liveWithSttp
    val serializer    = KryoSerialization.live
    val storage       = Storage.memory
    val sharding      = config ++ grpcPods ++ managerClient ++ storage ++ serializer >>> Sharding.live
    val grpcShardSvc  = config ++ sharding >>> GrpcShardingService.live
    sharding ++ grpcShardSvc
  }

  lazy val redisStg: ZLayer[ShardingConf with ShardRedisStgConf, Throwable, Sharding] = {
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
