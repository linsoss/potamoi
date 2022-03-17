package com.github.potamois.potamoi.testkit.akka

import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.{ActorRef, ActorSystem}
import com.typesafe.scalalogging.Logger
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.FiniteDuration

/**
 * Actor standard testkit enhanced trait for use with
 * [[akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit]].
 *
 * @author Al-assad
 */
trait STAkkaSpec extends AnyWordSpecLike with BeforeAndAfterEach {

  protected val log: Logger = Logger(getClass)

  /**
   * Create a TestProbe and use it directly on specified function.
   *
   * @example {{{
   * val actor = spawn(MyActor())
   * testProbe[Record] { probe =>
   *   actor ! GetValue(tp.ref)
   *   probe.expectMessage(Record(1, "Tulzscha"))
   * }
   * }}}
   */
  def testProbe[M](func: TestProbe[M] => Any)(implicit system: ActorSystem[_]): TestProbe[M] = {
    val probe = TestProbe[M]()
    func(probe)
    probe
  }

  /**
   * Shortened of [[testProbe]]
   */
  def probe[M](func: TestProbe[M] => Any)(implicit system: ActorSystem[_]): TestProbe[M] = testProbe(func)

  /**
   * Create a TestProbe and use it's ActorRef directly on specified function.
   * For example:
   *
   * @example {{{
   * val actor = spawn(MyActor())
   * testProbeRef[Record](actor ! GetValue(_)) expectMessage Record.default
   * }}}
   */
  def testProbeRef[M](func: ActorRef[M] => Any)(implicit system: ActorSystem[_]): TestProbe[M] = {
    val probe = TestProbe[M]()
    func(probe.ref)
    probe
  }

  /**
   * Shortened of [[testProbeRef]]
   */
  def probeRef[M](func: ActorRef[M] => Any)(implicit system: ActorSystem[_]): TestProbe[M] = testProbeRef(func)


  /**
   * Enhancement for TestProbe
   */
  implicit class RichTestProbe[M](probe: TestProbe[M]) {

    /**
     * Wrap the return of [[TestProbe.receiveMessage()]] to partition function.
     */
    def receiveMessagePf(assert: M => Any): M = {
      val r = probe.receiveMessage()
      assert(r)
      r
    }

    /**
     * Shortened of [[receiveMessagePf]]
     */
    def receivePf(assert: M => Any): M = receiveMessagePf(assert)

    /**
     * Wrap the return of [[TestProbe.receiveMessage(max: FiniteDuration)]]
     * to partition function.
     */
    def receiveMessagePfIn(max: FiniteDuration)(assert: M => Any): M = {
      val r = probe.receiveMessage(max)
      assert(r)
      r
    }

    /**
     * Shortened of [[receiveMessagePfIn]]
     */
    def receivePfIn(max: FiniteDuration)(assert: M => Any): M = receiveMessagePfIn(max)(assert)
  }


}
