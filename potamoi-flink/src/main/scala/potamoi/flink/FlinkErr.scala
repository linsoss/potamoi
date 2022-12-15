package potamoi.flink

import potamoi.common.SilentErr
import potamoi.flink.model.Fcid

/**
 * Flink error.
 */
sealed abstract class FlinkErr(msg: String, cause: Throwable = SilentErr) extends Throwable

object FlinkErr:
  case class ClusterNotFound(fcid: Fcid) extends FlinkErr(s"Flink cluster not found: ${fcid.show}")

/**
 * Data storage operation err.
 */
sealed abstract class DataStorageErr(msg: String, cause: Throwable) extends FlinkErr(msg, cause)

object DataStorageErr:
  case class ReadDataErr(cause: Throwable)   extends DataStorageErr("Fail to read data from storage", cause)
  case class UpdateDataErr(cause: Throwable) extends DataStorageErr("Fail to update data to storage", cause)

/**
 * Flink rest api error
 */
sealed abstract class FlinkRestErr(msg: String, cause: Throwable = SilentErr) extends FlinkErr(msg, cause)

object FlinkRestErr:
  case class RequestApiErr(method: String, uri: String, cause: Throwable) extends FlinkRestErr(s"Fail to request flink rest api: $method $uri", cause)
  case class JarNotFound(jarId: String)                                   extends FlinkRestErr(s"Flink jar not found: $jarId")
  case class JobNotFound(jobId: String)                                   extends FlinkRestErr(s"Flink job not found: $jobId")
  case class TriggerNotFound(triggerId: String)                           extends FlinkRestErr(s"Flink trigger not found: $triggerId")
  case class TaskmanagerNotFound(tmId: String)                            extends FlinkRestErr(s"Flink task manager not found: $tmId")
