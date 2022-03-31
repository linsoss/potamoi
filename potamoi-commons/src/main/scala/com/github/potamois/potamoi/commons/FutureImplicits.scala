package com.github.potamois.potamoi.commons


import scala.concurrent.{Await, Future, TimeoutException}
import scala.concurrent.duration.{Duration, DurationInt}
import scala.util.Try

/**
 * Enhancement function for scala Future.
 *
 * @author Al-assad
 */
object FutureImplicits {

  private val defaultTimeout: Duration = 30.seconds

  implicit class Wrapper[T](future: Future[T]) {

    /**
     * Await the result of the Future.
     *
     * @param timeout blocking timeout time
     * @throws TimeoutException when the call times out
     */
    @throws(classOf[TimeoutException])
    @throws(classOf[InterruptedException])
    def waitResult(timeout: Duration): T = Await.result(future, timeout)
    def waitResult: T = waitResult(defaultTimeout)

    /**
     * Await the result wrapped by [[Try]] of the Future.
     *
     * @param timeout blocking timeout time
     */
    def tryWaitResult(timeout: Duration): Try[T] = Try(Await.result(future, timeout))
    def tryWaitResult: Try[T] = tryWaitResult(defaultTimeout)

  }

  /**
   * Convenience method for Thread.sleep
   */
  def sleep(delay: Duration): Unit = Thread.sleep(delay.toMillis)

}


