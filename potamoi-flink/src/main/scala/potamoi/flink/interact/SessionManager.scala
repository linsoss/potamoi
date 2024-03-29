package potamoi.flink.interact

import akka.actor.typed.ActorRef
import potamoi.{uuids, PotaErr}
import potamoi.akka.{ActorOpErr, AkkaMatrix}
import potamoi.cluster.Address
import potamoi.flink.*
import potamoi.flink.model.interact.*
import potamoi.flink.model.FlinkTargetType
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.FlinkInteractErr.*
import potamoi.flink.interact.FlinkSqlInteractor.RetrieveSessionErr
import potamoi.flink.interact.SessionManager.*
import potamoi.flink.interpreter.{FlinkInterpreter, FlinkInterpreterActor}
import potamoi.flink.interpreter.FlinkInterpreter.ops
import potamoi.flink.interpreter.FlinkInterpreterActor.*
import potamoi.flink.storage.{FlinkDataStorage, InteractSessionStorage}
import potamoi.flink.FlinkErr.AkkaErr
import potamoi.zios.{someOrFailUnion, union}
import zio.{durationInt, IO, Task, UIO, ZIO}
import zio.ZIO.{fail, logDebug, logInfo, succeed, unit}
import zio.ZIOAspect.annotated
import zio.stream.ZStream

/**
 * Flink interactive session manager.
 */
trait SessionManager {

  /**
   * Create flink sql interactive session.
   */
  def create(sessionDef: InteractSessionSpec, flinkVer: FlinkMajorVer): IO[CreateSessionErr, SessionId]

  /**
   * Create SessionDef of interactive session entity.
   */
  def update(sessionId: String, sessionDef: InteractSessionSpec): IO[UpdateSessionErr, Unit]

  /**
   * Cancel current sqls execution plan of given session entity.
   */
  def cancel(sessionId: String): IO[SessionOpErr, Unit]

  /**
   * Close remote session entity of given session id.
   */
  def close(sessionId: String): IO[SessionOpErr, Unit]

  /**
   * List remote physical interpreter node of the given flink version.
   */
  def listRemoteInterpreter(flinkVer: FlinkMajorVer): IO[AkkaErr, Set[InterpreterNode]]

  /**
   * List all remote physical interpreter node.
   */
  def listAllRemoteInterpreter: IO[AkkaErr, Set[InterpreterNode]]

  /**
   * Interpreter session information query.
   */
  def session: InteractSessionStorage.Query

}

object SessionManager {
  type CreateSessionErr = (RemoteInterpreterNotYetLaunch | ResolveFlinkClusterEndpointErr | AkkaErr | FlinkDataStoreErr) with FlinkErr
  type UpdateSessionErr = (SessionNotFound | ResolveFlinkClusterEndpointErr | AkkaErr | FlinkDataStoreErr) with FlinkErr
  type SessionOpErr     = (RetrieveSessionErr | AkkaErr) with FlinkErr
}

/**
 * Default implementation
 */
class SessionManagerImpl(
    flinkConf: FlinkConf,
    observer: FlinkObserver,
    dataStore: InteractSessionStorage,
    interpreters: Map[FlinkMajorVer, ActorRef[FlinkInterpreter.Req]]
  )(using matrix: AkkaMatrix)
    extends SessionManager {

  private given FlinkRestEndpointType = flinkConf.restEndpointTypeInternal

  lazy val session = dataStore

  /**
   * Create flink sql interactive session.
   */
  override def create(sessionDef: InteractSessionSpec, flinkVer: FlinkMajorVer): IO[CreateSessionErr, String] = {
    for {
      // check if the corresponding version of the remote interpreter is active.
      isReady <- listRemoteInterpreter(flinkVer).map(_.nonEmpty)
      _       <- ZIO.fail(RemoteInterpreterNotYetLaunch(flinkVer)).when(!isReady)

      // send create session command to remote interpreter.
      sessionId <- succeed(uuids.genUUID32)
      sessDef   <- resolveSessionDef(sessionDef)
      _         <- dataStore.put(InteractSession(sessionId, flinkVer))

      _ <- logDebug("Call remote flink interactive session rpc command: Create(updateConflict=false)")
      _ <- interpreters(flinkVer)(sessionId).askZIO(Start(sessDef, updateConflict = false, _)).mapError(AkkaErr.apply)
    } yield sessionId
  }

  /**
   * Convert InteractSessionDef to SessionDef
   */
  private def resolveSessionDef(sessionDef: InteractSessionSpec): IO[ResolveFlinkClusterEndpointErr, SessionSpec] = {
    for {
      flinkSvcEpt <- (sessionDef.execType, sessionDef.remoteCluster) match {
                       case (FlinkTargetType.Remote, Some(fcid)) =>
                         observer.restEndpoint.getEnsure(fcid).mapError(err => ResolveFlinkClusterEndpointErr(fcid, err))
                       case _                                    => succeed(None)
                     }
      sessDef      = SessionSpec(
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
  override def update(sessionId: String, sessionDef: InteractSessionSpec): IO[UpdateSessionErr, Unit] =
    union[UpdateSessionErr, Unit] {
      for {
        session <- dataStore.get(sessionId).someOrFailUnion(SessionNotFound(sessionId))
        sessDef <- resolveSessionDef(sessionDef)
        _       <- logDebug("Call remote flink interactive session rpc command: Create(updateConflict=true)")
        _       <- interpreters(session.flinkVer)(sessionId).askZIO(Start(sessDef, updateConflict = true, _)).mapError(AkkaErr.apply)
      } yield ()
    } @@ annotated("sessionId" -> sessionId)

  /**
   * Cancel current sqls execution plan of given session entity.
   */
  override def cancel(sessionId: String): IO[SessionOpErr, Unit] =
    union[SessionOpErr, Unit] {
      for {
        session <- dataStore.get(sessionId).someOrFailUnion(SessionNotFound(sessionId))
        _       <- logDebug("Call remote flink interactive session rpc command: CancelCurrentHandles")
        _       <- interpreters(session.flinkVer)(sessionId).askZIO(Cancel.apply).mapError(AkkaErr.apply)
      } yield ()
    } @@ annotated("sessionId" -> sessionId)

  /**
   * Close remote session entity of given session id.
   */
  override def close(sessionId: String): IO[SessionOpErr, Unit] =
    union[SessionOpErr, Unit] {
      for {
        session <- dataStore.get(sessionId).someOrFailUnion(SessionNotFound(sessionId))
        _       <- logDebug("Call remote flink interactive session rpc command: Stop")
        _       <- interpreters(session.flinkVer)(sessionId).askZIO(Stop.apply).mapError(AkkaErr.apply)
        _       <- dataStore.rm(sessionId).retryN(3).ignore
      } yield ()
    } @@ annotated("sessionId" -> sessionId)

  /**
   * List remote interpreter of the given flink version.
   */
  override def listRemoteInterpreter(flinkVer: FlinkMajorVer): IO[AkkaErr, Set[InterpreterNode]] = {
    matrix
      .findReceptionist(FlinkInterpreter.ServiceKeys(flinkVer), timeout = Some(15.seconds))
      .mapBoth(
        AkkaErr.apply,
        { set => set.map { actorRef => InterpreterNode(flinkVer, Address(actorRef.path.address), actorRef.path.toString) } }
      )
  }

  /**
   * List all remote interpreter.
   */
  override def listAllRemoteInterpreter: IO[AkkaErr, Set[InterpreterNode]] = {
    ZIO
      .foreachPar(FlinkMajorVer.values)(listRemoteInterpreter)
      .map { rs =>
        rs.foldLeft(Set.empty[InterpreterNode]) { (acc, list) => acc ++ list }
      }
  }

}
