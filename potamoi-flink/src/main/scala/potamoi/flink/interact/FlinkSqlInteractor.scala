package potamoi.flink.interact

import com.devsisters.shardcake.{EntityType, Messenger, Sharding}
import potamoi.flink.{FlinkConf, FlinkDataStoreErr, FlinkErr, FlinkMajorVer}
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.protocol.{FlinkInterpEntity, FlinkInterpProto}
import potamoi.flink.storage.{FlinkDataStorage, InteractSessionStorage}
import potamoi.flink.FlinkInteractErr.SessionNotFound
import potamoi.times.given_Conversion_ScalaDuration_ZioDuration
import potamoi.uuids
import zio.{IO, ZIO, ZLayer}

type SessionId = String
type HandleId  = String

/**
 * Flink sql interactor that providing flink sql execution and
 * result view interaction.
 */
trait FlinkSqlInteractor:

  def manager: SessionManager
  def attach(sessionId: SessionId): IO[FlinkErr, SessionConnection]

object FlinkSqlInteractor:

  val live: ZLayer[FlinkDataStorage with Sharding with FlinkObserver with FlinkConf, Nothing, FlinkSqlInteractor] =
    ZLayer {
      for {
        flinkConf     <- ZIO.service[FlinkConf]
        flinkObserver <- ZIO.service[FlinkObserver]
        sharding      <- ZIO.service[Sharding]
        dataStore     <- ZIO.service[FlinkDataStorage].map(_.interact)
        rpcTimeout     = flinkConf.sqlInteract.rpcTimeout
        interpreters   = FlinkInterpEntity.adapters.map { case (ver, entity) =>
                           ver -> sharding.messenger(entity, Some(rpcTimeout))
                         }
      } yield Live(flinkConf, flinkObserver, dataStore, interpreters)
    }

  class Live(
      flinkConf: FlinkConf,
      flinkObserver: FlinkObserver,
      dataStore: InteractSessionStorage,
      interpreters: Map[FlinkMajorVer, Messenger[FlinkInterpProto]])
      extends FlinkSqlInteractor {

    lazy val manager: SessionManager = SessionManagerImpl(flinkConf, flinkObserver, dataStore, interpreters)

    def attach(sessionId: SessionId): IO[SessionNotFound | FlinkDataStoreErr | FlinkErr, SessionConnection] =
      for {
        session    <- dataStore.session.get(sessionId).someOrFail(SessionNotFound(sessionId))
        flinkVer    = session.flinkVer
        interpreter = interpreters(flinkVer)
      } yield SessionConnectionImpl(sessionId, flinkConf, interpreter)
  }
