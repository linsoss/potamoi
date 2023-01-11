package potamoi.flink.interp

import com.devsisters.shardcake.Replier
import potamoi.flink.FlinkInterpErr.InitSessionFail
import potamoi.flink.interp.model.{HandleFrame, SessionDef, SqlRsView}

type HandleId = String

sealed trait FlinkSqlExecCommand

object FlinkSqlExecCommand:
  case class InitEnv(sessDef: SessionDef, replier: Replier[Either[InitSessionFail, Unit]]) extends FlinkSqlExecCommand
  case class ExecStatement(statement: String, replier: Replier[HandleId])                  extends FlinkSqlExecCommand
  case class GetHandleFrame(handleId: HandleId, replier: Replier[Option[HandleFrame]])     extends FlinkSqlExecCommand
  case class ListHandleFrame(replier: Replier[List[HandleFrame]])                          extends FlinkSqlExecCommand
  case class GetResult(handleId: HandleId, replier: Replier[Option[SqlRsView]])            extends FlinkSqlExecCommand

class FlinkSqlExecutor {}
