package potamoi.flink.interp

import potamoi.common.PotaErr

sealed trait FlinkInterpErr extends PotaErr

object FlinkInterpErr:

  sealed trait ExecuteSqlErr extends FlinkInterpErr

  case class InitEnvErr(cause: Throwable)          extends FlinkInterpErr
  case object EnvNotYetReady                       extends FlinkInterpErr with ExecuteSqlErr
  case class ExecutorBusy(runningHandleId: String) extends FlinkInterpErr with ExecuteSqlErr
  case object HandleFound                          extends FlinkInterpErr

  case class ParseSqlErr(sql: String, cause: Throwable)            extends FlinkInterpErr with ExecuteSqlErr
  case class BannedOperation(opClzName: String)                    extends FlinkInterpErr with ExecuteSqlErr
  case class ExecOperationErr(opClzName: String, cause: Throwable) extends FlinkInterpErr with ExecuteSqlErr
  case class PotaInternalErr(cause: PotaErr)                       extends FlinkInterpErr with ExecuteSqlErr
