package com.github.potamois.potamoi.commons

import com.github.potamois.potamoi.testkit.STSpec

class FiniteQueueSpec extends STSpec {

  "FiniteQueue" should {

    "discard elements automatically" in {
      val queue = FiniteQueue[Int](3)
      queue += 1
      queue += 2
      queue += 3
      queue shouldBe Seq(1, 2, 3)
      queue += 4
      queue shouldBe Seq(2, 3, 4)

      queue.enqueue(5)
      queue shouldBe Seq(3, 4, 5)

      queue ++= Seq(6, 7)
      queue shouldBe Seq(5, 6, 7)
    }

    "prepend element correctly" in {
      val queue = FiniteQueue[Int](3)
      queue += 1
      queue.+=:(2)
      queue shouldBe Seq(2, 1)
      queue += 3
      queue shouldBe Seq(2, 1, 3)
      queue.+=:(4)
      queue shouldBe Seq(2, 1, 3)
    }

  }

}
