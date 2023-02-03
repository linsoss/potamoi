package potamoi.flink.model.interact

import potamoi.{curTs, KryoSerializable}
import potamoi.flink.FlinkInterpreterErr
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
    error: Option[HandleErr] = None)
    extends KryoSerializable

case class HandleErr(err: FlinkInterpreterErr, stack: String)

case class HandleStatusView(handleId: String, status: HandleStatus, submitAt: Long) extends KryoSerializable

enum HandleStatus:
  case Wait
  case Run
  case Finish
  case Fail
  case Cancel

object HandleStatuses:
  import HandleStatus.*
  private val endStatuses                  = Set(Finish, Fail, Cancel)
  def isEnd(status: HandleStatus): Boolean = endStatuses.contains(status)
