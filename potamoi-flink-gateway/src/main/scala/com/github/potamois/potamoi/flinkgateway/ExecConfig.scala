package com.github.potamois.potamoi.flinkgateway


import com.github.potamois.potamoi.flinkgateway.EvictStrategy.EvictStrategy
import com.github.potamois.potamoi.flinkgateway.ExecMode.ExecMode

import scala.collection.mutable

case class ExecConfig(executeMode: ExecMode = ExecMode.LOCAL,
                      remoteAddr: Option[RemoteAddr] = None,
                      flinkConfig: Map[String, String] = Map.empty,
                      resultCollectStrategy: ResultCollectStrategy = ResultCollectStrategy.default) {

  def toEffectiveExecConfig: EffectiveExecConfig = ExecConfig.convergeExecConfig(this)
}

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

  def localEnv(flinkConfig: Map[String, String] = Map.empty,
               resultCollectStrategy: ResultCollectStrategy = ResultCollectStrategy.default): ExecConfig = {
    ExecConfig(ExecMode.LOCAL, None, flinkConfig, resultCollectStrategy)
  }

  def remoteEnv(remoteAddr: RemoteAddr,
                flinkConfig: Map[String, String] = Map.empty,
                resultCollectStrategy: ResultCollectStrategy = ResultCollectStrategy.default): ExecConfig = {
    ExecConfig(ExecMode.REMOTE, Some(remoteAddr), flinkConfig, resultCollectStrategy)
  }

  // todo initialize from hocon
  lazy val DEFAULT_FLINK_CONFIG = Map(
    "rest.retry.max-attempts" -> "1",
  )

  /**
   * Converge effective configuration from ExecConfig to EffectiveExecConfig
   * especially for flink configuration.
   *
   * @param config should not be null
   */
  def convergeExecConfig(config: ExecConfig): EffectiveExecConfig = {
    // flink config
    val convergedConfig = {
      // base on default flink config
      val rsConf = mutable.Map(DEFAULT_FLINK_CONFIG.toSeq: _*)
      // set execution mode
      if (config.executeMode != null) rsConf += "execution.target" -> config.executeMode.toString
      // set remote address
      config.remoteAddr match {
        case Some(RemoteAddr(host, port)) =>
          rsConf += "rest.address" -> host
          rsConf += "rest.port" -> port.toString
      }
      // override with user-defined flink config
      rsConf ++= config.flinkConfig
      rsConf.toMap
    }
    // resultCollectStrategy
    val collectStrategy = Option(config.resultCollectStrategy) getOrElse ResultCollectStrategy.default
    EffectiveExecConfig(convergedConfig, collectStrategy)
  }

}
