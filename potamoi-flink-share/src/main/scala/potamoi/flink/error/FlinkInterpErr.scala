package potamoi.flink.error

import potamoi.PotaErr
import potamoi.common.Syntax.toPrettyString
import potamoi.flink.error.FlinkInterpErr.ExecuteSqlErr
import potamoi.flink.error.FlinkInterpErr

/**
 * Flink interpreter error.
 */
sealed trait FlinkInterpErr extends PotaErr

object FlinkInterpErr:

  sealed trait RetrieveResultNothing          extends FlinkInterpErr
  case class HandleNotFound(handleId: String) extends RetrieveResultNothing
  case class ResultNotFound(handleId: String) extends RetrieveResultNothing

  sealed trait ExecuteSqlErr                                       extends FlinkInterpErr
  case class CreateTableEnvironmentErr(cause: Throwable)           extends ExecuteSqlErr
  case class ParseSqlErr(sql: String, cause: Throwable)            extends ExecuteSqlErr
  case class BannedOperation(opClzName: String)                    extends ExecuteSqlErr
  case class ExecOperationErr(opClzName: String, cause: Throwable) extends ExecuteSqlErr
  case class BeCancelled(handleId: String)                         extends ExecuteSqlErr

  case class SplitSqlScriptErr(cause: Throwable) extends FlinkInterpErr
