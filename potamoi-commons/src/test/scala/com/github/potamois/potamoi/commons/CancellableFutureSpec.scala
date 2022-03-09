package com.github.potamois.potamoi.commons

import com.github.potamois.potamoi.testkit.STSpec
import org.scalatest.concurrent.{ScalaFutures, Waiters}

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{CancellationException, ExecutionContext}

class CancellableFutureSpec extends STSpec with Waiters with ScalaFutures {

  "CancellableFuture" should {

    "complete with result" in {
      val f = CancellableFuture("test")
      f.futureValue shouldBe "test"
      f.isCompleted shouldBe true
      f.isCancelled shouldBe false
    }

    "be cancellable" in {
      val f = CancellableFuture {
        Thread.sleep(1000)
      }
      f.cancel(false) shouldBe true
      f.failed.futureValue.isInstanceOf[CancellationException]
      f.isCompleted shouldBe true
      f.isCancelled shouldBe true
    }

    "immediately cancel pending task" in {
      val ec = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
      // Fully utilize the ExecutionContext
      val w = new Waiter()
      CancellableFuture {
        w.await()
      }(ec)

      val executed = new AtomicBoolean()
      val f = CancellableFuture {
        executed.set(true)
      }(ec)

      f.cancel(false) shouldBe true
      w.dismiss()
      Thread.sleep(500)
      executed.get() shouldBe false
      f.failed.futureValue.isInstanceOf[CancellationException]
      f.isCompleted shouldBe true
      f.isCancelled shouldBe true
    }

    "not support repeated cancellation" in {
      val f = CancellableFuture {
        Thread.sleep(1000)
      }
      f.cancel(false) shouldBe true
      f.cancel(false) shouldBe false
    }

    "be interruptable" in {
      val waiter = new Waiter()
      val f = CancellableFuture {
        try {
          Thread.sleep(1000)
        } catch {
          case _: InterruptedException => waiter.dismiss()
        }
      }
      Thread.sleep(100)

      f.cancel(true) shouldBe true
      f.failed.futureValue.isInstanceOf[CancellationException]
      waiter.await()
    }
  }

}
