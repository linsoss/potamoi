package com.github.potamois.potamoi.flinkgateway


import com.github.potamois.potamoi.flinkgateway.EvictStrategy.EvictStrategy
import com.github.potamois.potamoi.flinkgateway.ExecMode.ExecMode

import scala.collection.mutable

case class ExecConfig(executeMode: ExecMode = ExecMode.LOCAL,
                      remoteAddr: Option[RemoteAddr] = None,
                      flinkConfig: Map[String, String] = Map.empty,
                      resultCollectStrategy: ResultCollectStrategy = ResultCollectStrategy.default)

case class EffectiveExecConfig(flinkConfig: Map[String, String] = Map.empty,
                               resultCollectStrategy: ResultCollectStrategy = ResultCollectStrategy.default)

case class RemoteAddr(host: String, port: Int)

case class ResultCollectStrategy(evictStrategy: EvictStrategy, size: Long)

object ResultCollectStrategy {
  val default: ResultCollectStrategy = ResultCollectStrategy(EvictStrategy.WINDOW, 1000L)
}

// noinspection TypeAnnotation
object ExecMode extends Enumeration {
  type ExecMode = Value
  val LOCAL = Value("local")
  val REMOTE = Value("remote")
}

object EvictStrategy extends Enumeration {
  type EvictStrategy = Value
  val BARRIER, WINDOW = Value
}


object ExecConfig {
  // todo initialize from hocon
  lazy val DEFAULT_FLINK_CONFIG = Map(
    "rest.retry.max-attempts" -> "1"
  )

  /**
   * Converge effective configuration from ExecConfig to EffectiveExecConfig
   * especially for flink configuration.
   * @param config should not be null
   */
  def convergeExecConfig(config: ExecConfig): EffectiveExecConfig = {
    // base on default flink config
    val convergedConfig = mutable.Map(DEFAULT_FLINK_CONFIG.toSeq: _*)
    // set execution mode
    if (config.executeMode != null) {
      convergedConfig += "execution.target" -> config.executeMode.toString
    }
    // set remote address
    config.remoteAddr match {
      case Some(RemoteAddr(host, port)) =>
        convergedConfig += "rest.address" -> host
        convergedConfig += "rest.port" -> port.toString
    }
    // override with user-defined flink config
    convergedConfig ++= config.flinkConfig
    EffectiveExecConfig(
      convergedConfig.toMap,
      config.resultCollectStrategy)
  }

}
