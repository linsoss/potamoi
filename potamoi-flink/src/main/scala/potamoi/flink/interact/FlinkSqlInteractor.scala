package potamoi.flink.interact

import potamoi.akka.ActorCradle
import potamoi.flink.{FlinkConf, FlinkDataStoreErr, FlinkErr, FlinkMajorVer}
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.storage.{FlinkDataStorage, InteractSessionStorage}
import potamoi.flink.FlinkInteractErr.SessionNotFound
import potamoi.flink.interact.FlinkSqlInteractor.RetrieveSessionErr
import potamoi.flink.interpreter.FlinkInterpreterPier
import potamoi.fs.refactor.RemoteFsOperator
import potamoi.logger.LogConf
import potamoi.times.given_Conversion_ScalaDuration_ZIODuration
import potamoi.{uuids, EarlyLoad}
import potamoi.zios.someOrFailUnion
import zio.{IO, URIO, ZIO, ZLayer}

type SessionId = String
type HandleId  = String

/**
 * Flink sql interactor that providing flink sql execution and
 * result view interaction.
 */
trait FlinkSqlInteractor:

  def manager: SessionManager
  def attach(sessionId: SessionId): IO[RetrieveSessionErr, SessionConnection]

object FlinkSqlInteractor extends EarlyLoad[FlinkSqlInteractor]:

  type RetrieveSessionErr = (SessionNotFound | FlinkDataStoreErr) with FlinkErr

  override def active: URIO[FlinkSqlInteractor, Unit] = ZIO.service[FlinkSqlInteractor].unit

  val live: ZLayer[
    RemoteFsOperator with FlinkConf with LogConf with ActorCradle with FlinkDataStorage with FlinkObserver,
    Throwable,
    FlinkSqlInteractor] = ZLayer {
    for {
      actorCradle      <- ZIO.service[ActorCradle]
      flinkConf        <- ZIO.service[FlinkConf]
      flinkObserver    <- ZIO.service[FlinkObserver]
      dataStore        <- ZIO.service[FlinkDataStorage].map(_.interact)
      interpreters     <- FlinkInterpreterPier.activeAll
      given ActorCradle = actorCradle

    } yield new FlinkSqlInteractor {
      lazy val manager: SessionManager = SessionManagerImpl(flinkConf, flinkObserver, dataStore, interpreters)

      def attach(sessionId: SessionId): IO[RetrieveSessionErr, SessionConnection] =
        for {
          session    <- dataStore.get(sessionId).someOrFailUnion(SessionNotFound(sessionId))
          flinkVer    = session.flinkVer
          interpreter = interpreters(flinkVer)
        } yield SessionConnectionImpl(sessionId, flinkConf, interpreter)
    }
  }
