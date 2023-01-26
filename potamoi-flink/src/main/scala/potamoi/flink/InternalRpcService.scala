package potamoi.flink

import com.devsisters.shardcake.{EntityType, Sharding}
import potamoi.common.Ack
import potamoi.flink.model.interact.{RemoteClusterEndpoint, SessionDef}
import potamoi.flink.model.FlinkTargetType
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.protocol.{InternalRpcEntity, InternalRpcProto}
import potamoi.flink.storage.FlinkDataStorage
import potamoi.rpc.RpcService
import potamoi.sharding.ShardRegister
import zio.{Dequeue, IO, RIO, Scope, Tag, UIO, URIO, ZIO, ZLayer}
import zio.ZIO.succeed
import zio.direct.*

import scala.util.Failure

/**
 * Internal system rpc service for potamoi-flink module.
 * see: [[potamoi.flink.protocol.InternalRpcProto]]
 */
object InternalRpcService {

  val live: ZLayer[FlinkDataStorage, Nothing, InternalRpcService] = ZLayer {
    for {
      dataStore <- ZIO.service[FlinkDataStorage]
    } yield InternalRpcService(dataStore)
  }

  def registerEntities: URIO[InternalRpcService with Sharding with Scope, Unit] =
    ZIO.serviceWithZIO[InternalRpcService](_.registerEntities)
}

import InternalRpcProto.*

class InternalRpcService(dataStore: FlinkDataStorage) extends RpcService[InternalRpcProto](InternalRpcEntity) {

  def handleMessage(message: InternalRpcProto): URIO[Sharding, Unit] = message match {
    case RegisterFlinkInterpreter(pod, replier)           => dataStore.interact.pod.put(pod).as(Ack).either.flatMap(replier.reply)
    case UnregisterFlinkInterpreter(flinkVer, host, port) => dataStore.interact.pod.rm(flinkVer, host, port).ignore
  }
}
