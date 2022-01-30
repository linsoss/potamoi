package com.github.potamois.potamoi.commons

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, CanAwait, CancellationException, ExecutionContext, Future, Promise, TimeoutException}
import scala.util.{Failure, Try}

object CancellableFuture {
  def apply[T](body: => T)(implicit executor: ExecutionContext): CancellableFuture[T] = new CancellableFuture(body)
}

/**
 * A future that supports cancellation and interruption, even when backed by a ForkJoinPool.
 */
class CancellableFuture[T](body: => T)(implicit executor: ExecutionContext) extends Future[T] {
  private val promise = Promise[T]()
  private var thread: Thread = null
  private val cancelled = new AtomicBoolean()

  promise tryCompleteWith Future {
    if (!promise.isCompleted) {
      this.synchronized {
        thread = Thread.currentThread
      }
      try {
        body
      } finally {
        this.synchronized {
          // Clears the interrupt flag
          Thread.interrupted()
          thread = null
        }
      }
    } else
      null.asInstanceOf[T]
  }

  /**
   * Attempts to cancel the future. Cancellation succeeds if the future is not yet complete or cancelled. A cancelled
   * future will have a result of Failure(CancellationException).
   *
   * @param interrupt Whether to interrupt the running Future
   * @return True if the execution was cancelled
   */
  def cancel(interrupt: Boolean): Boolean = {
    if (!promise.isCompleted && cancelled.compareAndSet(false, true)) {
      promise.tryComplete(Failure(new CancellationException()))
      if (interrupt) {
        this.synchronized {
          if (thread != null)
            thread.interrupt()
        }
      }
      true
    } else
      false
  }

  /**
   * Returns whether the future was cancelled.
   */
  def isCancelled: Boolean = cancelled.get()

  override def onComplete[U](f: Try[T] => U)(implicit executor: ExecutionContext): Unit = promise.future.onComplete(f)

  override def isCompleted: Boolean = promise.future.isCompleted

  override def value: Option[Try[T]] = promise.future.value

  @throws(classOf[TimeoutException])
  @throws(classOf[InterruptedException])
  def ready(atMost: Duration)(implicit permit: CanAwait): this.type = CancellableFuture[T](Await.result(promise.future, atMost)).asInstanceOf

  @throws(classOf[Exception])
  def result(atMost: Duration)(implicit permit: CanAwait): T = promise.future.result(atMost)

  override def transform[S](f: Try[T] => Try[S])(implicit executor: ExecutionContext): Future[S] = promise.future.transform(f)

  override def transformWith[S](f: Try[T] => Future[S])(implicit executor: ExecutionContext): Future[S] = promise.future.transformWith(f)
}
