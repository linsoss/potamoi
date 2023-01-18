package potamoi.flink.model.interact

import potamoi.curTs
import potamoi.flink.error.FlinkInterpErr
import zio.Cause

/**
 * Execution frames of the Flink sql executor, each frame records
 * the execution status, launch time, execute result and other
 * information of the received sql.
 */
case class HandleFrame(
    handleId: String,
    sql: String,
    status: HandleStatus,
    submitAt: Long = curTs,
    jobId: Option[String] = None,
    result: Option[SqlResultView] = None,
    error: Option[Cause[FlinkInterpErr]] = None)

case class HandleStatusView(handleId: String, status: HandleStatus, submitAt: Long)

enum HandleStatus:
  case Wait
  case Run
  case Finish
  case Fail
  case Cancel
