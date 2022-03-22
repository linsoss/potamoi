package com.github.potamois.potamoi.gateway.flink.interact

import com.github.potamois.potamoi.gateway.flink.Error

/**
 * Reasons for refusing to execute the sql plan request.
 *
 * @author Al-assad
 */
sealed trait ExecReqReject {
  def reason: String
}

/**
 * The executor is busy and there is currently a sql statement that is
 * still being executed.
 *
 * @param reason  rejection reason
 * @param startTs start timestamp of the sql statements execution plan in process
 */
case class BusyInProcess(reason: String = "The executor is busy in process, please cancel it first or wait until it is complete",
                         startTs: Long) extends ExecReqReject


/**
 * Reject to execute sql statement because the sql statement submitted is empty.
 */
case class StatementIsEmpty(reason: String = "The sql statements is empty") extends ExecReqReject


/**
 * Initialize Flink environment failure.
 *
 * @param reason rejection reason
 * @param err    exception
 */
case class InitFlinkEnvFailure(reason: String = "Fail to initialize the flink environment",
                               err: Error) extends ExecReqReject
object InitFlinkEnvFailure {
  def apply(cause: Throwable): InitFlinkEnvFailure = InitFlinkEnvFailure(err = Error(cause.getMessage, cause))
}


/**
 * Fail to load the thirty dependencies to current classloader.
 *
 * @param reason   rejection reason
 * @param depsURLs the dependency urls
 * @param err      exception
 */
case class LoadDepsToClassLoaderFailure(reason: String = "Fail to load dependencies into Classloader",
                                        depsURLs: Seq[String],
                                        err: Error) extends ExecReqReject
object LoadDepsToClassLoaderFailure {
  def apply(_depsURLs: Seq[String], cause: Throwable): LoadDepsToClassLoaderFailure = LoadDepsToClassLoaderFailure(
    reason = s"Fail to load dependencies into Classloader: ${_depsURLs.mkString(", ")}",
    depsURLs = _depsURLs,
    Error(cause.getMessage, cause))
}

/**
 * Unknown failure during the execution of the sql statement plan.
 */
case class ExecutionFailure(reason: String = "Fail to execute the sql statement plan", err: Error) extends ExecReqReject
object ExecutionFailure {
  def apply(cause: Throwable): ExecutionFailure = ExecutionFailure(err = Error(cause.getMessage, cause))
}

