package potamoi.flink.interact

import potamoi.{uuids, EarlyLoad}
import potamoi.akka.AkkaMatrix
import potamoi.flink.{FlinkConf, FlinkDataStoreErr, FlinkErr, FlinkMajorVer}
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.storage.{FlinkDataStorage, InteractSessionStorage}
import potamoi.flink.FlinkInteractErr.SessionNotFound
import potamoi.flink.interact.FlinkSqlInteractor.RetrieveSessionErr
import potamoi.flink.interpreter.FlinkInterpreterPier
import potamoi.fs.refactor.RemoteFsOperator
import potamoi.logger.LogConf
import potamoi.times.given_Conversion_ScalaDuration_ZIODuration
import potamoi.zios.someOrFailUnion
import zio.{IO, URIO, ZIO, ZLayer}

type SessionId = String
type HandleId  = String

/**
 * Flink sql interactor that providing flink sql execution and
 * result view interaction.
 */
trait FlinkSqlInteractor {

  /**
   * Remote flink sql interpreters manager.
   */
  def manager: SessionManager

  /**
   * Connect to the flink interpreter for the given session id.
   */
  def attach(sessionId: SessionId): IO[(SessionNotFound | FlinkDataStoreErr) with FlinkErr, SessionConnection]
}

object FlinkSqlInteractor extends EarlyLoad[FlinkSqlInteractor] {

  val live
      : ZLayer[RemoteFsOperator with FlinkConf with LogConf with AkkaMatrix with FlinkDataStorage with FlinkObserver, Throwable, FlinkSqlInteractor] =
    ZLayer {
      for {
        actorCradle     <- ZIO.service[AkkaMatrix]
        flinkConf       <- ZIO.service[FlinkConf]
        flinkObserver   <- ZIO.service[FlinkObserver]
        dataStore       <- ZIO.service[FlinkDataStorage].map(_.interact)
        interpreters    <- FlinkInterpreterPier.activeAll
        given AkkaMatrix = actorCradle

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

  override def active: URIO[FlinkSqlInteractor, Unit] = ZIO.service[FlinkSqlInteractor].unit
}
