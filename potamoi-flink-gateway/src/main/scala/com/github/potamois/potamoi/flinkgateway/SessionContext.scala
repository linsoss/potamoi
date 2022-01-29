package com.github.potamois.potamoi.flinkgateway

import com.github.potamois.potamoi.flinkgateway.EvictStrategy.EvictStrategy
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

import scala.collection.mutable

// todo covert all specific params to flinkConfig including executeMode, etra dependencies or so.

case class SessionContext(sessionId: String,
                          config: SessionContextConfig = SessionContextConfig.default,
                          tableEnv: StreamTableEnvironment)

case class SessionContextConfig(var flinkConfig: mutable.Map[String, String] = mutable.Map.empty,
                                var resultCollectStrategy: ResultCollectStrategy = ResultCollectStrategy.default)

case class ResultCollectStrategy(evictStrategy: EvictStrategy, size: Long)

object EvictStrategy extends Enumeration {
  type EvictStrategy = Value
  val BARRIER, WINDOW = Value
}

object SessionContextConfig{
  // todo init from hocon config
  def default: SessionContextConfig = SessionContextConfig()
}

object ResultCollectStrategy {
  def default: ResultCollectStrategy = ResultCollectStrategy(EvictStrategy.WINDOW, 1000L)
}




