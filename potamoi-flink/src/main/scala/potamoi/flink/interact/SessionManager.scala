package potamoi.flink.interact

import com.devsisters.shardcake.{Messenger, Sharding}
import potamoi.flink.*
import potamoi.flink.model.interact.*
import potamoi.flink.model.FlinkTargetType
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.protocol.{FlinkInterpEntity, FlinkInterpProto}
import potamoi.flink.FlinkInteractErr.{InterpreterNotYetRegistered, ResolveFlinkClusterEndpointErr, RpcFailure, SessionNotFound}
import potamoi.flink.storage.{FlinkDataStorage, InteractSessionStorage}
import potamoi.rpc.Rpc.narrowRpcErr
import potamoi.uuids
import zio.{IO, Task, UIO, ZIO}
import zio.ZIO.{logDebug, logInfo, succeed}
import zio.ZIOAspect.annotated

/**
 * Flink interactive session manager.
 */
trait SessionManager:

  def create(sessionDef: InteractSessionDef, flinkVer: FlinkMajorVer): IO[FlinkErr, SessionId]
  def update(sessionId: String, sessionDef: InteractSessionDef): IO[FlinkErr, Unit]
  def cancel(sessionId: String): IO[FlinkErr, Unit]
  def close(sessionId: String): IO[FlinkErr, Unit]

  def session: InteractSessionStorage.SessionStorage.Query
  def pod: InteractSessionStorage.PodStorage.Query

/**
 * Default implementation
 */
class SessionManagerImpl(
    flinkConf: FlinkConf,
    observer: FlinkObserver,
    dataStore: InteractSessionStorage,
    interpreters: Map[FlinkMajorVer, Messenger[FlinkInterpProto]])
    extends SessionManager:

  private given FlinkRestEndpointType = flinkConf.restEndpointTypeInternal

  lazy val session = dataStore.session
  lazy val pod     = dataStore.pod

  /**
   * Create flink sql interactive session.
   */
  override def create(
      sessionDef: InteractSessionDef,
      flinkVer: FlinkMajorVer)
      : IO[InterpreterNotYetRegistered | RpcFailure | ResolveFlinkClusterEndpointErr | FlinkDataStoreErr | FlinkErr, SessionId] = {
    for {
      // check if the corresponding version of the remote interpreter is registered.
      _         <- dataStore.pod.exists(flinkVer).flatMap {
                     case true  => ZIO.unit
                     case false => ZIO.fail(InterpreterNotYetRegistered(flinkVer))
                   }
      // send create session command to remote interpreter.
      sessionId <- succeed(uuids.genUUID32)
      sessDef   <- resolveSessionDef(sessionDef)
      _         <- dataStore.session.put(InteractSession(sessionId, flinkVer))
      _         <- logDebug("Call remote flink interactive session rpc command: Create(updateConflict=false)")
      _         <- interpreters(flinkVer)
                     .send(sessionId)(FlinkInterpProto.Start(sessDef, false, _))
                     .narrowRpcErr
    } yield sessionId
  }

  /**
   * Convert InteractSessionDef to SessionDef
   */
  private def resolveSessionDef(sessionDef: InteractSessionDef): IO[ResolveFlinkClusterEndpointErr, SessionDef] = {
    for {
      flinkSvcEpt <- (sessionDef.execType, sessionDef.remoteCluster) match {
                       case (FlinkTargetType.Remote, Some(fcid)) =>
                         observer.restEndpoint.getEnsure(fcid).mapError(err => ResolveFlinkClusterEndpointErr(fcid, err))
                       case _                                    => succeed(None)
                     }
      sessDef      = SessionDef(
                       execType = sessionDef.execType,
                       execMode = sessionDef.execMode,
                       remoteEndpoint = flinkSvcEpt.map(ept => RemoteClusterEndpoint(ept.chooseHost, ept.port)),
                       jobName = sessionDef.jobName,
                       localJars = sessionDef.localJars,
                       clusterJars = sessionDef.clusterJars,
                       parallelism = sessionDef.parallelism,
                       extraProps = sessionDef.extraProps,
                       resultStore = sessionDef.resultStore,
                       allowSinkOperation = sessionDef.allowSinkOperation
                     )
    } yield sessDef
  }

  /**
   * Create SessionDef of interactive session entity.
   */
  override def update(
      sessionId: String,
      sessionDef: InteractSessionDef): IO[SessionNotFound | FlinkDataStoreErr | ResolveFlinkClusterEndpointErr | RpcFailure | FlinkErr, Unit] = {
    for {
      session <- dataStore.session.get(sessionId).someOrFail(SessionNotFound(sessionId))
      sessDef <- resolveSessionDef(sessionDef)
      _       <- logDebug("Call remote flink interactive session rpc command: Create(updateConflict=true)")
      _       <- interpreters(session.flinkVer)
                   .send(sessionId)(FlinkInterpProto.Start(sessDef, true, _))
                   .narrowRpcErr
    } yield ()
  } @@ annotated("sessionId" -> sessionId)

  /**
   * Cancel current sqls execution plan of given session entity.
   */
  override def cancel(sessionId: String): IO[SessionNotFound | FlinkDataStoreErr | RpcFailure | FlinkErr, Unit] = {
    for {
      session <- dataStore.session.get(sessionId).someOrFail(SessionNotFound(sessionId))
      _       <- logDebug("Call remote flink interactive session rpc command: CancelCurrentHandles")
      _       <- interpreters(session.flinkVer).send(sessionId)(FlinkInterpProto.CancelCurrentHandles.apply).narrowRpcErr
    } yield ()
  } @@ annotated("sessionId" -> sessionId)

  /**
   * Close remote session entity of given session id.
   */
  override def close(sessionId: String): IO[SessionNotFound | FlinkDataStoreErr | RpcFailure | FlinkErr, Unit] = {
    for {
      session <- dataStore.session.get(sessionId).someOrFail(SessionNotFound(sessionId))
      _       <- logDebug("Call remote flink interactive session rpc command: Stop")
      _       <- interpreters(session.flinkVer)
                   .send(sessionId)(FlinkInterpProto.Stop.apply)
                   .narrowRpcErr
                   .unit
    } yield ()
  } @@ annotated("sessionId" -> sessionId)
