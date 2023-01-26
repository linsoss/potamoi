package potamoi.flink.protocol

import com.devsisters.shardcake.{EntityType, Replier}
import potamoi.flink.{FlinkDataStoreErr, FlinkMajorVer}
import potamoi.flink.model.interact.{InteractSessionDef, InterpreterPod, SessionDef}
import potamoi.flink.model.Fcid
import potamoi.PotaErr
import potamoi.common.Ack

import scala.util.Try

/**
 * Internal system exchange data protocol based on Shardcake.
 */
sealed trait InternalRpcProto

object InternalRpcEntity extends EntityType[InternalRpcProto]("flinkInternalRpc")

object InternalRpcProto {

  case class RegisterFlinkInterpreter(pod: InterpreterPod, replier: Replier[Either[FlinkDataStoreErr, Ack.type]]) extends InternalRpcProto
  case class UnregisterFlinkInterpreter(flinkVer: FlinkMajorVer, host: String, port: Int)                         extends InternalRpcProto

  //  case class UpdateSessionStatus(
  //      sessionId: String,
  //      status: InteractSessionStatus,
  //      replier: Replier[Either[DataStoreErr, Unit]])
  //      extends InternalRpcProto

}
