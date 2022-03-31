package com.github.potamois.potamoi.gateway.flink.interact

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import com.github.potamois.potamoi.akka.testkit.{STAkkaSpec, defaultConfig}
import com.github.potamois.potamoi.commons.FutureImplicits.Wrapper
import com.github.potamois.potamoi.gateway.flink.FlinkVersion.SystemFlinkVerSign
import com.github.potamois.potamoi.gateway.flink.interact.QuickSqlCases.explainSqls

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

  var manager: ActorRef[FsiSessManager.Command] = _

  override protected def beforeEach(): Unit = {
    manager = spawn(FsiSessManager(autoRestart = false))
  }

  override protected def afterEach(): Unit = {
    manager ! FsiSessManager.Terminate
  }


  "FsiSessManager's single node behavior" should {

    "create session -> execute sql -> close session" in {
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
    }

    "create multiple session" in {

    }

    "create session with invalid Flink version sign" in {

    }

    "create session repeatedly" in {

    }

    "forward command with ack reply" in {

    }

    "forward command to non-existent session-id" in {

    }

    "close session while the fsi-executor is still in process" in {

    }

    "close non-existent session-id" in {

    }

  }

}
