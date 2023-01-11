package potamoi.flink.interp.model

import potamoi.flink.interp.model.HandleStatus

case class HandleFrame(
    sql: String,
    state: HandleStatus,
    submitAt: Long,
    runAt: Option[Long] = None,
    endAt: Option[Long] = None,
    jobId: Option[String] = None,
    error: Option[ErrorDetail] = None)

enum HandleStatus:
  case Wait
  case Run
  case Finish
  case Fail
  case Cancel

object HandleStatuses:
  import HandleStatus.*
  lazy val endStates                      = Array(Finish, Fail, Cancel)
  def isEnd(state: HandleStatus): Boolean = endStates.contains(state)

case class ErrorDetail(message: String, causeStack: String)
