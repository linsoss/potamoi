package potamoi.flink.protocol

import com.devsisters.shardcake.*
import potamoi.common.Ack
import potamoi.flink.FlinkInteractErr.*
import potamoi.flink.FlinkMajorVer
import potamoi.flink.model.interact.*
import zio.{Cause, UIO}

/**
 * Flink sql interpreter shards proto.
 */
trait FlinkInterpProto

object FlinkInterpEntity:

  object V116 extends EntityType[FlinkInterpProto]("flinkInterp116")
  object V115 extends EntityType[FlinkInterpProto]("flinkInterp115")

  val adapters: Map[FlinkMajorVer, EntityType[FlinkInterpProto]] = Map(
    FlinkMajorVer.V116 -> V116,
    FlinkMajorVer.V115 -> V115
  )

object FlinkInterpProto:

  private type Reply[E, A] = Replier[Either[E, A]]

  case class Start(sessionDef: SessionDef, updateConflict: Boolean = false, replier: Replier[Ack.type]) extends FlinkInterpProto
  case class Stop(replier: Replier[Ack.type])                                                           extends FlinkInterpProto
  case object Terminate                                                                                 extends FlinkInterpProto
  case class CancelCurrentHandles(replier: Replier[Ack.type])                                           extends FlinkInterpProto
  case class GetOverview(replier: Replier[SessionOverview])                                             extends FlinkInterpProto

  case class CompleteSql(
      sql: String,
      position: Int,
      replier: Reply[SessionNotYetStarted, List[String]])
      extends FlinkInterpProto

  case class SubmitSqlAsync(
      sql: String,
      handleId: String,
      replier: Reply[SessionNotYetStarted, Ack.type])
      extends FlinkInterpProto

  case class SubmitSqlScriptAsync(
      sqlScript: String,
      replier: Reply[SessionNotYetStarted | FailToSplitSqlScript, List[ScripSqlSign]])
      extends FlinkInterpProto

  case class RetrieveResultPage(
      handleId: String,
      page: Int,
      pageSize: Int,
      replier: Reply[SessionNotYetStarted | SessionHandleNotFound, Option[SqlResultPageView]])
      extends FlinkInterpProto

  case class RetrieveResultOffset(
      handleId: String,
      offset: Long,
      chunkSize: Int,
      replier: Reply[SessionNotYetStarted | SessionHandleNotFound, Option[SqlResultOffsetView]])
      extends FlinkInterpProto

  case class ListHandleId(replier: Reply[SessionNotYetStarted, List[String]])               extends FlinkInterpProto
  case class ListHandleStatus(replier: Reply[SessionNotYetStarted, List[HandleStatusView]]) extends FlinkInterpProto
  case class ListHandleFrame(replier: Reply[SessionNotYetStarted, List[HandleFrame]])       extends FlinkInterpProto

  case class GetHandleStatus(
      handleId: String,
      replier: Reply[SessionNotYetStarted | SessionHandleNotFound, HandleStatusView])
      extends FlinkInterpProto

  case class GetHandleFrame(
      handleId: String,
      replier: Reply[SessionNotYetStarted | SessionHandleNotFound, HandleFrame])
      extends FlinkInterpProto
