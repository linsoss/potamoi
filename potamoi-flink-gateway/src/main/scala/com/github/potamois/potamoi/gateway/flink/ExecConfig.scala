package com.github.potamois.potamoi.gateway.flink

import com.github.potamois.potamoi.akka.CborSerializable
import com.github.potamois.potamoi.gateway.flink.EvictStrategy.EvictStrategy
import com.github.potamois.potamoi.gateway.flink.ExecMode.ExecMode

import scala.collection.mutable

/**
 * Flink interactive operation execution configuration.
 * It is recommended to use the factory creation method in the ExecConfig object
 * such as ExecConfig.localEnv or ExecConfig.remoteEnv.
 *
 * @param executeMode           execution mode of flink job, see [[ExecMode]]
 * @param remoteAddr            remote flink cluster rest endpoint when use [[ExecMode.REMOTE]]
 * @param flinkConfig           extra flink configuration
 * @param resultCollectStrategy result collector strategy, see [[ResultCollectStrategy]]
 * @author Al-assad
 *
 */
case class ExecConfig(executeMode: ExecMode = ExecMode.LOCAL,
                      remoteAddr: Option[RemoteAddr] = None,
                      flinkConfig: Map[String, String] = Map.empty,
                      resultCollectStrategy: ResultCollectStrategy = ResultCollectStrategy.default) extends CborSerializable {

  def toEffectiveExecConfig: EffectiveExecConfig = ExecConfig.convergeExecConfig(this)
}

/**
 * Effective execution configuration of Flink interactive operation.
 *
 * @param flinkConfig           extra flink configuration
 * @param resultCollectStrategy result collector strategy, see [[ResultCollectStrategy]]
 * @author Al-assad
 */
case class EffectiveExecConfig(flinkConfig: Map[String, String] = Map.empty,
                               resultCollectStrategy: ResultCollectStrategy = ResultCollectStrategy.default)

/**
 * Flink remote cluster address.
 *
 * @param host hostname of flink jobmanager rest endpoint
 * @param port port of flink jobmanager rest endpoint
 * @author Al-assad
 */
case class RemoteAddr(host: String, port: Int)

/**
 * Strategy to collect result of Flink interactive operation.
 *
 * @param evictStrategy eviction strategy, see [[ResultCollectStrategy]]
 * @param size          result buffer size
 * @author Al-assad
 */
case class ResultCollectStrategy(evictStrategy: EvictStrategy, size: Long)

object ResultCollectStrategy {
  val default: ResultCollectStrategy = ResultCollectStrategy(EvictStrategy.WINDOW, 1000L)
}

/**
 * Eviction strategy of result buffer, see [[ResultCollectStrategy]].
 * The eviction strategy determines how to evict the excess elements when
 * the result reaches the buffer limit:
 *
 * 1) BARRIER
 * No new elements would be accepted, and the result collection process would
 * be terminated.
 *
 * 2) WINDOW: Evict the oldest element in the buffer, while continuing to accept
 * the new elements.
 *
 * @author Al-assad
 */
object EvictStrategy extends Enumeration {
  type EvictStrategy = Value
  val BARRIER, WINDOW = Value
}

/**
 * Execution mode of Flink job.
 */
// noinspection TypeAnnotation
object ExecMode extends Enumeration {
  type ExecMode = Value

  /** LOCAL mode will launch a miniCluster within current JVM. */
  val LOCAL = Value("local")

  /** REMOTE mode includes all Flink session mode likes standalone,
   * session-yarn, session-kubernetes. */
  val REMOTE = Value("remote")
}


/**
 * @author Al-assad
 */
object ExecConfig {

  /**
   * Create a local-execution environment config instance.
   */
  def localEnv(flinkConfig: Map[String, String] = Map.empty,
               resultCollectStrategy: ResultCollectStrategy = ResultCollectStrategy.default): ExecConfig = {
    ExecConfig(ExecMode.LOCAL, None, flinkConfig, resultCollectStrategy)
  }

  /**
   * Create a remote-execution environment config instance.
   */
  def remoteEnv(remoteAddr: RemoteAddr,
                flinkConfig: Map[String, String] = Map.empty,
                resultCollectStrategy: ResultCollectStrategy = ResultCollectStrategy.default): ExecConfig = {
    ExecConfig(ExecMode.REMOTE, Some(remoteAddr), flinkConfig, resultCollectStrategy)
  }

  /**
   * default flink configuration
   * todo initialize from hocon
   */
  lazy val DEFAULT_FLINK_CONFIG = Map(
    "rest.retry.max-attempts" -> "1",
  )

  /**
   * Converge effective configuration from ExecConfig to EffectiveExecConfig especially
   * for flink configuration.
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