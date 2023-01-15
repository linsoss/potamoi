package potamoi.flink.interp

import potamoi.PotaErr
import potamoi.flink.interp.FlinkInterpErr.ExecuteSqlErr

sealed trait FlinkInterpErr extends PotaErr

object FlinkInterpErr:

  sealed trait RetrieveResultNothing extends FlinkInterpErr
  case object HandleNotFound         extends RetrieveResultNothing
  case object ResultNotFound         extends RetrieveResultNothing

  sealed trait ExecuteSqlErr                                       extends FlinkInterpErr
  case class CreateTableEnvironmentErr(cause: Throwable)           extends ExecuteSqlErr
  case class ParseSqlErr(sql: String, cause: Throwable)            extends ExecuteSqlErr
  case class BannedOperation(opClzName: String)                    extends ExecuteSqlErr
  case class ExecOperationErr(opClzName: String, cause: Throwable) extends ExecuteSqlErr
