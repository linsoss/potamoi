package potamoi.sharding

import com.devsisters.shardcake.{GrpcPods, ManagerConfig, ShardManager}
import com.devsisters.shardcake.interfaces.{PodsHealth, Storage}
import potamoi.kubernetes.K8sConf
import potamoi.sharding.store.{ShardcakeRedisStorage, ShardRedisStoreConf}
import zio.ZLayer

/**
 * Shardcake [[ShardManager]] layer.
 */
//noinspection DuplicatedCode
object ShardManagers:

  lazy val test = memStoreLocalPod
  lazy val live = redisStoreK8sPod

  /**
   * storage: memory, pod-health: local
   */
  lazy val memStoreLocalPod: ZLayer[ShardManagerConf, Throwable, ShardManager with ManagerConfig] = {
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
  lazy val redisStoreLocalPod: ZLayer[ShardManagerConf with ShardRedisStoreConf, Throwable, ShardManager with ManagerConfig] = {
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
  lazy val redisStoreK8sPod: ZLayer[ShardManagerConf with ShardRedisStoreConf with K8sConf, Throwable, ShardManager with ManagerConfig] = {
    val managerConfig = ZLayer.service[ShardManagerConf].project(_.toManagerConfig)
    val grpcConfig    = ZLayer.service[ShardManagerConf].project(_.toGrpcConfig)
    val grpcPods      = grpcConfig >>> GrpcPods.live
    val podHealth     = KubernetesPodsHealth.live
    val storage       = ShardcakeRedisStorage.live
    managerConfig ++ storage ++ grpcPods ++ podHealth >>> ShardManager.live ++ managerConfig
  }