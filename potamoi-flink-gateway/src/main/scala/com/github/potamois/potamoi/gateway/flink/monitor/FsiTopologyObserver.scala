package com.github.potamois.potamoi.gateway.flink.monitor

/**
 * Observer for monitoring Flink interaction actor topology.
 * The actor being Observed including:
 *  - [[com.github.potamois.potamoi.gateway.flink.interact.FsiSessManager]]
 *  - [[com.github.potamois.potamoi.gateway.flink.interact.FsiExecutor]]
 *
 * @author Al-assad
 */
object FsiTopologyObserver {

  sealed trait Command

  sealed trait Internal

  // todo use Distributed Data as storage

}
