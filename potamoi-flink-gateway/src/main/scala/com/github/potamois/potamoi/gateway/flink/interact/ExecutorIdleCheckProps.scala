package com.github.potamois.potamoi.gateway.flink.interact

import com.github.potamois.potamoi.commons.PotaConfig
import com.typesafe.config.Config
import com.github.potamois.potamoi.commons.JdkDurationConversions._

import scala.concurrent.duration.{FiniteDuration, MINUTES, SECONDS}

/**
 * Idle check config for FsiExecutor.
 *
 * @param timeout   maximum idle timeout limit
 * @param interval  interval to check for idle executors
 * @param initDelay initial delay before the first check
 * @author Al-assad
 */
case class ExecutorIdleCheckProps(timeout: FiniteDuration, interval: FiniteDuration, initDelay: FiniteDuration) {
  lazy val timeoutMillis: Long = timeout.toMillis
}

object FsiExecutorIdleCheckProps {

  val default: ExecutorIdleCheckProps = from(PotaConfig.root)

  /**
   * Read config from parent config.
   */
  def from(parent: Config): ExecutorIdleCheckProps = {
    val config = parent.getConfig("potamoi.flink-gateway.sql-interaction.fsi-executor-idle-check")
    ExecutorIdleCheckProps(
      timeout = config.getDuration("timeout").asScala(MINUTES),
      interval = config.getDuration("interval").asScala(SECONDS),
      initDelay = config.getDuration("init-delay").asScala(SECONDS))
  }

}
