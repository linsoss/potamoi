package potamoi.flink

import com.devsisters.shardcake.{EntityType, Sharding}
import potamoi.flink.model.interact.{RemoteClusterEndpoint, SessionDef}
import potamoi.flink.model.FlinkTargetType
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.protocol.{InternalRpcEntity, InternalRpcProto}
import potamoi.flink.storage.FlinkDataStorage
import potamoi.rpc.RpcService
import potamoi.sharding.ShardRegister
import zio.{Dequeue, IO, RIO, Scope, Tag, UIO, URIO, ZIO}
import zio.ZIO.succeed
import zio.direct.*

import scala.util.Failure

/**
 * Internal system rpc service for potamoi-flink module.
 * see: [[potamoi.flink.protocol.InternalRpcProto]]
 * todo continue
 */
object InternalRpcService {

  val live: ZIO[FlinkConf with FlinkDataStorage with FlinkObserver, Nothing, InternalRpcService] =
    for {
      flinkConf <- ZIO.service[FlinkConf]
      dataStore <- ZIO.service[FlinkDataStorage]
      observer  <- ZIO.service[FlinkObserver]
    } yield InternalRpcService(flinkConf, dataStore, observer)
}

import InternalRpcProto.*

class InternalRpcService(
    flinkConf: FlinkConf,
    dataStore: FlinkDataStorage,
    observer: FlinkObserver)
    extends RpcService[InternalRpcProto](InternalRpcEntity) {

  def handleMessage(message: InternalRpcProto): URIO[Sharding, Unit] = ???

//  private def getInteractSessionDef(sessionId: String): IO[DataStoreErr | RetrieveRemoteClusterEndpointErr, Option[SessionDef]] = defer {
//    val interactSessionDef = dataStore.interactSession.get(sessionId).map(_.map(_.definition)).run
//    interactSessionDef match
//      case None              => succeed(None).run
//      case Some(interactDef) =>
//        // convert remote fcid to RemoteClusterEndpoint
//        val remoteEndpoint: Option[RemoteClusterEndpoint] = interactDef.remoteCluster match
//          case None       => succeed(None).run
//          case Some(fcid) =>
//            observer.restEndpoint
//              .getEnsure(fcid)
//              .mapBoth(RetrieveRemoteClusterEndpointErr(fcid, _), _.map(ept => RemoteClusterEndpoint(ept.chooseHost, ept.port)))
//              .run
//        succeed(
//          SessionDef(
//            execType = interactDef.execType,
//            execMode = interactDef.execMode,
//            remoteEndpoint = remoteEndpoint,
//            jobName = interactDef.jobName,
//            localJars = interactDef.localJars,
//            clusterJars = interactDef.clusterJars,
//            parallelism = interactDef.parallelism,
//            extraProps = interactDef.extraProps,
//            resultStore = interactDef.resultStore,
//            allowSinkOperation = interactDef.allowSinkOperation
//          ))
//  }

}
