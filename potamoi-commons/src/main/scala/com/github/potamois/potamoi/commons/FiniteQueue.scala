package com.github.potamois.potamoi.commons

import scala.collection.mutable

/**
 * Queue of finite size, when the elements exceeds the maximum size,
 * the element at the head of the queue will be discarded.
 *
 * @author Al-assad
 */
@SerialVersionUID(7856455850368838173L)
class FiniteQueue[A](limit: Int) extends mutable.Queue[A] {

  protected def evictElem(): Unit = while (length > limit) dequeue()

  override def enqueue(elems: A*): Unit = {
    super.enqueue(elems: _*)
    evictElem()
  }

  override def +=(elem: A): FiniteQueue.this.type = {
    appendElem(elem)
    evictElem()
    this
  }

  override def +=:(elem: A): FiniteQueue.this.type = {
    prependElem(elem)
    evictElem()
    this
  }
}

object FiniteQueue {
  def apply[A](limit: Int): FiniteQueue[A] = new FiniteQueue[A](limit)
}



