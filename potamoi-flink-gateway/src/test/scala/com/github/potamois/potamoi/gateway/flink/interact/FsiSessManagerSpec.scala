package com.github.potamois.potamoi.gateway.flink.interact

import akka.actor.testkit.typed.scaladsl.{ScalaTestWithActorTestKit, TestProbe}
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import akka.util.Timeout
import com.github.potamois.potamoi.akka.testkit.{STAkkaSpec, defaultConfig}
import com.github.potamois.potamoi.commons.FutureImplicits.Wrapper
import com.github.potamois.potamoi.gateway.flink.FlinkVersion.SystemFlinkVerSign
import com.github.potamois.potamoi.gateway.flink.interact.QuickSqlCases.{explainSqls, selectSqls}

import scala.concurrent.duration.DurationInt

/**
 * Testing the behavior of [[FsiSessManager]] on a single node.
 *
 * todo create test suite for remote and local mode
 *
 * @note Some of following cases would take a long time.
 * @author Al-assad
 */
//noinspection TypeAnnotation
class FsiSessManagerSpec extends ScalaTestWithActorTestKit(defaultConfig) with STAkkaSpec {

  import FsiExecutor._
  import FsiSessManager._

  def newFsiSessManager(test: ActorRef[FsiSessManager.Command] => Any): Unit = {
    val manager = spawn(FsiSessManager(autoRestart = false))
    test(manager)
    manager ! FsiSessManager.Terminate
    testKit.stop(manager)
  }

  "FsiSessManager's single node behavior" should {

    "create session -> execute sql -> close session" in newFsiSessManager { manager =>
      // create session
      val sessionId = (manager ? (CreateSession(SystemFlinkVerSign, _))).waitResult.getOrElse(fail)
      probeRef[Boolean] {
        manager ! ExistSession(sessionId, _)
      } expectMessage true

      // execute sql
      probeRef[RejectableDone] {
        manager ! sessionId -> ExecuteSqls(explainSqls.sql, props, _)
      } receivePF explainSqls.passExecuteSqls

      // get execute result
      probeRef[ExecutionPlanResult] {
        manager ! sessionId -> GetExecPlanResult(_)
      } receivePF explainSqls.passGetExecPlanResult

      // close session
      manager ! CloseSession(sessionId)
      eventually {
        probeRef[Boolean] {
          manager ! ExistSession(sessionId, _)
        } expectMessage false
      }
    }


    "create session with invalid Flink version sign" in newFsiSessManager { manager =>
      probeRef[RejectOrSessionId] {
        manager ! CreateSession(144514, _)
      } receivePF {
        case Left(re) => re.isInstanceOf[UnsupportedFlinkVersion] shouldBe true
        case Right(_) => fail
      }
    }

    "forward command with ack reply" in newFsiSessManager { manager =>
      val sessionId = (manager ? (CreateSession(SystemFlinkVerSign, _))).waitResult.getOrElse(fail)
      val sqlProbe = TestProbe[RejectableDone]
      val ackProbe = TestProbe[Boolean]

      manager ! sessionId -> ExecuteSqls(explainSqls.sql, props, sqlProbe.ref) -> ackProbe.ref
      ackProbe.expectMessage(true)
      manager ! "114514" -> ExecuteSqls(explainSqls.sql, props, sqlProbe.ref) -> ackProbe.ref
      ackProbe.expectMessage(false)
    }

    "close session while the fsi-executor is still in process" in newFsiSessManager { manager =>
      val sessionId = (manager ? (CreateSession(SystemFlinkVerSign, _))).waitResult.getOrElse(fail)
      manager ! sessionId -> ExecuteSqls(selectSqls.sql, props, system.ignoreRef)
      probeRef[Boolean](manager ! ExistSession(sessionId, _)).expectMessage(true)

      manager ! CloseSession(sessionId)
      eventually {
        probeRef[Boolean](manager ! ExistSession(sessionId, _)).expectMessage(false)
      }
    }

    "close non-existent session-id" in newFsiSessManager { manager =>
      manager ! CloseSession("114514")
      eventually {
        probeRef[Boolean](manager ! ExistSession("114514", _)).expectMessage(false)
      }
    }

    "create multiple session" in newFsiSessManager { manager =>
      implicit val timeout: Timeout = 10.seconds
      val sessionIds = (1 to 5).map { _ =>
        sleep(5.millis)
        (manager ? (CreateSession(SystemFlinkVerSign, _))).waitResult.getOrElse(fail)
      }
      sessionIds.foreach { sessionId =>
        //        manager ! sessionId -> SubscribeState(spawn(ExecRsChangeEventPrinter(sessionId)))
        probeRef[Boolean] {
          manager ! ExistSession(sessionId, _)
        } expectMessage true

        probeRef[RejectableDone] {
          manager ! sessionId -> ExecuteSqls(explainSqls.sql, props, _)
        }.receivePFIn(10.seconds) {
          explainSqls.passExecuteSqls
        }
        probeRef[ExecutionPlanResult] {
          manager ! sessionId -> GetExecPlanResult(_)
        } receivePF explainSqls.passGetExecPlanResult
      }
      sessionIds.foreach { sessionId =>
        sleep(5.millis)
        manager ! CloseSession(sessionId)
      }
      sessionIds.foreach { sessionId =>
        eventually {
          probeRef[Boolean] {
            manager ! ExistSession(sessionId, _)
          } expectMessage false
        }
      }
    }

  }

}
