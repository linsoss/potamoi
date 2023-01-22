package potamoi.flink.interact

import com.devsisters.shardcake.{Messenger, Sharding}
import potamoi.flink.{FlinkConf, FlinkInteractErr, FlinkRestEndpointType}
import potamoi.flink.FlinkInterpreterErr.{ExecOperationErr, ExecuteSqlErr, RetrieveResultNothing}
import potamoi.flink.model.interact.*
import potamoi.flink.model.FlinkTargetType
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.protocol.{FlinkInterpEntity, FlinkInterpProto}
import potamoi.flink.FlinkInteractErr.{CreateSessionReqErr, ResolveFlinkClusterEndpointErr, RpcFailure}
import potamoi.rpc.Rpc
import potamoi.uuids
import potamoi.rpc.Rpc.narrowRpcErr
import zio.{IO, Task, UIO}
import zio.ZIO.{logDebug, logInfo, succeed}
import zio.ZIOAspect.annotated

/**
 * Flink interactive session manager.
 */
trait SessionManager:

  // todo
  //  def list: UIO[List[SessionId]]
  def create(sessionDef: InteractSessionDef): IO[CreateSessionReqErr, SessionId]
  def update(sessionId: String, sessionDef: InteractSessionDef): IO[CreateSessionReqErr, Unit]
  def cancel(sessionId: String): IO[RpcFailure, Unit]
  def close(sessionId: String): IO[RpcFailure, Unit]

/**
 * Default implementation
 */
class SessionManagerImpl(flinkConf: FlinkConf, observer: FlinkObserver, interpreter: Messenger[FlinkInterpProto]) extends SessionManager:

  private given FlinkRestEndpointType = flinkConf.restEndpointTypeInternal

  /**
   * Create flink sql interactive session.
   */
  override def create(sessionDef: InteractSessionDef): IO[CreateSessionReqErr, SessionId] = {
    for {
      sessionId <- succeed(uuids.genUUID32)
      sessDef   <- resolveSessionDef(sessionDef)
      _         <- logDebug("Call remote flink interactive session rpc command: Create(updateConflict=false)")
      _         <- interpreter.send(sessionId)(FlinkInterpProto.Start(sessDef, false, _)).narrowRpcErr
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
  override def update(sessionId: String, sessionDef: InteractSessionDef): IO[CreateSessionReqErr, Unit] = {
    for {
      sessDef <- resolveSessionDef(sessionDef)
      _       <- logDebug("Call remote flink interactive session rpc command: Create(updateConflict=true)")
      _       <- interpreter.send(sessionId)(FlinkInterpProto.Start(sessDef, true, _)).narrowRpcErr
    } yield ()
  } @@ annotated("sessionId" -> sessionId)

  /**
   * Cancel current sqls execution plan of given session entity.
   */
  override def cancel(sessionId: String): IO[RpcFailure, Unit] = {
    for {
      _ <- logDebug("Call remote flink interactive session rpc command: CancelCurrentHandles")
      _ <- interpreter.send(sessionId)(FlinkInterpProto.CancelCurrentHandles.apply).narrowRpcErr
    } yield ()
  } @@ annotated("sessionId" -> sessionId)

  /**
   * Close remote session entity of given session id.
   */
  override def close(sessionId: String): IO[RpcFailure, Unit] = {
    for {
      _ <- logDebug("Call remote flink interactive session rpc command: Stop")
      _ <- interpreter.send(sessionId)(FlinkInterpProto.Stop.apply).narrowRpcErr.unit
    } yield ()
  } @@ annotated("sessionId" -> sessionId)
