package com.github.potamois.potamoi.gateway.flink.interact

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.github.potamois.potamoi.akka.testkit.{STAkkaSpec, defaultConfig}
import com.github.potamois.potamoi.gateway.flink.interact.QuickSqlCases.explainSqls
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration.DurationDouble

/**
 * Idle timeout specification for [[FsiSerialExecutor]].
 *
 * @note the following cases would take a long time.
 * @author Al-assad
 */
object FsiSerialExecutorIdleCheckSpec {
  def config: Config = ConfigFactory
    .parseString(
      """
        |potamoi.flink-gateway.sql-interaction.fsi-executor-idle-check {
        |  timeout = 1m
        |  interval = 5s
        |  init-delay = 5s
        |}
        |""".stripMargin)
    .withFallback(defaultConfig)
}

class FsiSerialExecutorIdleCheckSpec extends ScalaTestWithActorTestKit(FsiSerialExecutorIdleCheckSpec.config) with STAkkaSpec {

  "FsiSerialExecutor idle check" should {

    "shutdown when idle over idle timeout limit" in {
      val executor = spawn(FsiSerialExecutor("114514"))
      val watch = createTestProbe[FsiExecutor.Command]()
      executor ! FsiExecutor.ExecuteSqls(explainSqls.sql, ExecProps(), system.ignoreRef)
      sleep(1.5.minutes)
      watch.expectTerminated(executor)
    }

    "shutdown when do nothing over idle timeout limit" in {
      val executor = spawn(FsiSerialExecutor("114515"))
      val watch = createTestProbe[FsiExecutor.Command]()
      sleep(1.5.minutes)
      watch.expectTerminated(executor)
    }

    "keep active within idle timeout limit" in {
      val executor = spawn(FsiSerialExecutor("114516"))
      val watch = createTestProbe[FsiExecutor.Command]()
      sleep(30.seconds)
      intercept[AssertionError] {
        watch.expectTerminated(executor)
      }
    }

  }


}
