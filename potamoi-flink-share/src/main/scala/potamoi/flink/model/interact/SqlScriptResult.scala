package potamoi.flink.model.interact

import potamoi.flink.error.FlinkInterpErr.ExecuteSqlErr
import zio.stream.Stream

/**
 * Flink sql script submission result.
 */
case class SqlScriptResult(handles: List[ScripSqlSign], rsWatchStream: Stream[ExecuteSqlErr, SqlResult])

case class ScripSqlSign(handleId: String, sql: String)