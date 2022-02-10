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

  val defaultTimeout: Duration = 30.seconds

  implicit class FutureWrapper[T](future: Future[T]) {
    /**
     * Await the result of the Future.
     *
     * @param timeout blocking timeout time
     * @throws TimeoutException when the call times out
     */
    @throws(classOf[TimeoutException])
    def waitResult(timeout: Duration = defaultTimeout): T = Await.result(future, timeout)

    /**
     * Await the result wrapped by [[Try]] of the Future.
     *
     * @param timeout blocking timeout time
     */
    def tryWaitResult(timeout: Duration = defaultTimeout): Try[T] = Try(Await.result(future, timeout))

  }

}


