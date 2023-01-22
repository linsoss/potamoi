package potamoi.flink.interact

import com.devsisters.shardcake.Sharding
import potamoi.flink.FlinkConf
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.protocol.FlinkInterpEntity
import potamoi.uuids
import potamoi.times.given_Conversion_ScalaDuration_ZioDuration
import zio.{ZIO, ZLayer}

type SessionId = String
type HandleId  = String

/**
 * Flink sql interactor that providing flink sql execution and
 * result view interaction.
 * todo multiple flink version supports.
 */
trait FlinkSqlInteractor:
  def manager: SessionManager
  def attach(sessionId: SessionId): SessionConnection

object FlinkSqlInteractor:

  def live: ZLayer[Sharding with FlinkObserver with FlinkConf, Nothing, FlinkSqlInteractor] =
    ZLayer {
      for {
        flinkConf      <- ZIO.service[FlinkConf]
        flinkObserver  <- ZIO.service[FlinkObserver]
        sharding       <- ZIO.service[Sharding]
        rpcTimeout      = flinkConf.sqlInteract.rpcTimeout
        interpreterV116 = sharding.messenger(FlinkInterpEntity.V116, Some(rpcTimeout))
        interpreterV115 = sharding.messenger(FlinkInterpEntity.V115, Some(rpcTimeout))
      } yield new FlinkSqlInteractor:
        lazy val manager: SessionManager                    = SessionManagerImpl(flinkConf, flinkObserver, interpreterV116)
        def attach(sessionId: SessionId): SessionConnection = SessionConnectionImpl(sessionId, flinkConf, interpreterV116)
    }
