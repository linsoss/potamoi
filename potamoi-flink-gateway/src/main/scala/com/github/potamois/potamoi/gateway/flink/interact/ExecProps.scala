package com.github.potamois.potamoi.gateway.flink.interact

import com.github.potamois.potamoi.commons.{PotaConfig, RichMutableMap}
import com.github.potamois.potamoi.gateway.flink.interact.EvictStrategy.EvictStrategy
import com.github.potamois.potamoi.gateway.flink.interact.ExecMode.ExecMode

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions

/**
 * Flink interactive operation execution configuration.
 * It is recommended to use the factory creation method in the ExecConfig object
 * such as ExecProps.localEnv or ExecProps.remoteEnv.
 *
 * @param executeMode execution mode of flink job, see [[ExecMode]]
 * @param remoteAddr  remote flink cluster rest endpoint when use [[ExecMode.REMOTE]]
 * @param flinkConfig extra flink configuration
 * @param flinkDeps   extra flink dependencies or udf jars paths
 * @param rsCollectSt result collector strategy, see [[RsCollectStrategy]]
 * @author Al-assad
 *
 */
case class ExecProps(executeMode: ExecMode = ExecMode.LOCAL,
                     remoteAddr: Option[RemoteAddr] = None,
                     flinkConfig: Map[String, String] = Map.empty,
                     flinkDeps: Seq[String] = Seq.empty,
                     rsCollectSt: RsCollectStrategy = RsCollectStrategy.default) {

  def toEffectiveExecProps: EffectiveExecProps = ExecProps.convergeExecConfig(this)
}

/**
 * Effective execution configuration of Flink interactive operation.
 *
 * @param flinkConfig extra flink configuration
 * @param flinkDeps   extra flink dependencies jar paths
 * @param rsCollectSt result collector strategy, see [[RsCollectStrategy]]
 * @author Al-assad
 */
case class EffectiveExecProps(flinkConfig: Map[String, String] = Map.empty,
                              flinkDeps: Seq[String] = Seq.empty,
                              rsCollectSt: RsCollectStrategy = RsCollectStrategy.default) {
  // update flink config
  def updateFlinkConfig(updated: mutable.Map[String, String] => Unit): EffectiveExecProps = {
    val tmpMap = mutable.Map(flinkConfig.toSeq: _*)
    updated(tmpMap)
    copy(flinkConfig = tmpMap.toMap)
  }
}

/**
 * Flink remote cluster address.
 *
 * @param host hostname of flink jobmanager rest endpoint
 * @param port port of flink jobmanager rest endpoint
 * @author Al-assad
 */
case class RemoteAddr(host: String, port: Int)

object RemoteAddr {
  // implicit conversion
  implicit def toRemoteAddr(pair: (String, Int)): RemoteAddr = RemoteAddr(pair._1, pair._2)
}

/**
 * Strategy to collect result of Flink interactive operation.
 *
 * @param evictSt eviction strategy, see [[RsCollectStrategy]]
 * @param limit   result buffer size
 * @author Al-assad
 */
case class RsCollectStrategy(evictSt: EvictStrategy, limit: Int)

object RsCollectStrategy {
  val default: RsCollectStrategy = RsCollectStrategy(EvictStrategy.DROP_HEAD, 1000)

  // implicit conversion
  implicit def toRsCollectStrategy(pair: (EvictStrategy, Int)): RsCollectStrategy = RsCollectStrategy(pair._1, pair._2)
}

/**
 * Eviction strategy of result buffer, see [[RsCollectStrategy]].
 * The eviction strategy determines how to evict the excess elements when
 * the result reaches the buffer limit:
 *
 * 1) DROP_TAIL
 * No new elements would be accepted, and the result collection process would
 * be terminated.
 *
 * 2) DROP_HEAD: Evict the oldest element in the buffer, while continuing to accept
 * the new elements.
 *
 * @author Al-assad
 */
object EvictStrategy extends Enumeration {
  type EvictStrategy = Value
  val DROP_HEAD, DROP_TAIL = Value
}

/**
 * Execution mode of Flink job.
 */
//noinspection TypeAnnotation
object ExecMode extends Enumeration {
  type ExecMode = Value

  // LOCAL mode will launch a miniCluster within current JVM.
  val LOCAL = Value("local")

  // REMOTE mode includes all Flink session mode likes standalone,
  // session-yarn, session-kubernetes.
  val REMOTE = Value("remote")
}


/**
 * @author Al-assad
 */
object ExecProps {

  /**
   * Create a local-execution environment config instance.
   * Refer to [[ExecProps]] for params details.
   */
  def localEnv(flinkConfig: Map[String, String] = Map.empty,
               flinkDeps: Seq[String] = Seq.empty,
               rsCollectSt: RsCollectStrategy = RsCollectStrategy.default): ExecProps =
    ExecProps(ExecMode.LOCAL, None, flinkConfig, flinkDeps, rsCollectSt)

  /**
   * Create a remote-execution environment config instance.
   * Refer to [[ExecProps]] for params details.
   */
  def remoteEnv(remoteAddr: RemoteAddr,
                flinkConfig: Map[String, String] = Map.empty,
                flinkDeps: Seq[String] = Seq.empty,
                rsCollectSt: RsCollectStrategy = RsCollectStrategy.default): ExecProps =
    ExecProps(ExecMode.REMOTE, Some(remoteAddr), flinkConfig, flinkDeps, rsCollectSt)

  /**
   * Default flink configuration for interactive query scenario.
   */
  lazy val DEFAULT_FLINK_CONFIG: Map[String, String] =
    PotaConfig.root.getConfig("potamoi.flink-gateway.sql-interaction.default-flink-config")
      .entrySet().asScala.map(e => e.getKey -> e.getValue.unwrapped.toString).toMap


  // PotaConfig.root.getConfig("potamoi.flink-gateway.fsi.default-flink-config")

  /**
   * Converge effective configuration from ExecConfig to EffectiveExecConfig especially
   * for flink configuration.
   */
  def convergeExecConfig(config: ExecProps): EffectiveExecProps = {
    // flink config
    val convergedConfig = {
      // base on default flink config
      val rsConf = mutable.Map(DEFAULT_FLINK_CONFIG.toSeq: _*)
      // soft set execution mode
      rsConf ?+= "execution.target" -> config.executeMode.toString
      // set remote address
      config.remoteAddr.foreach { addr =>
        rsConf += "rest.address" -> addr.host
        rsConf += "rest.port" -> addr.port.toString
      }
      // override with user-defined flink config
      rsConf ++= config.flinkConfig
      // force attached mode on
      rsConf ++= Map(
        "execution.attached" -> "true",
        "execution.shutdown-on-attached-exit" -> "true")
      rsConf.toMap
    }
    // other config
    val collectStrategy = Option(config.rsCollectSt).getOrElse(RsCollectStrategy.default)
    val flinkDeps = Option(config.flinkDeps).getOrElse(Seq.empty)
    EffectiveExecProps(convergedConfig, flinkDeps, collectStrategy)
  }

}
