package potamoi.flink.interp.model

import potamoi.flink.interp.model.HandleStatus
import potamoi.flink.interp.FlinkInterpErr
import potamoi.curTs

/**
 * Execution frames of the Flink sql executor, each frame records
 * the execution status, launch time and other information of the
 * received sql.
 */
case class HandleFrame(
    handleId: String,
    sql: String,
    status: HandleStatus,
    runAt: Long = curTs,
    jobId: Option[String] = None)

/**
 * History frame, only when a frame is accepted by the sql executor and
 * is properly executed will be converted into a history frame and stored
 * temporarily in the sql executor.
 */
case class HistHandleFrame(
    frame: HandleFrame,
    result: SqlResultView,
    error: Option[FlinkInterpErr] = None)

enum HandleStatus:
  case Wait
  case Run
  case Finish
  case Fail
  case Cancel
