package potamoi.flink.protocol

import com.devsisters.shardcake.{EntityType, Replier}
import potamoi.flink.model.interact.{InteractSessionDef, InteractSessionStatus, SessionDef}
import potamoi.flink.FlinkDataStoreErr
import potamoi.flink.model.Fcid
import potamoi.PotaErr

import scala.util.Try

/**
 * Internal system exchange data protocol based on Shardcake.
 * todo continue
 */
sealed trait InternalRpcProto

object InternalRpcEntity extends EntityType[InternalRpcProto]("flinkInternalRpc")

object InternalRpcProto {

//  case class UpdateSessionStatus(
//      sessionId: String,
//      status: InteractSessionStatus,
//      replier: Replier[Either[DataStoreErr, Unit]])
//      extends InternalRpcProto

}
