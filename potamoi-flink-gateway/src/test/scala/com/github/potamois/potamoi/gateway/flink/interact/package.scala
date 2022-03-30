package com.github.potamois.potamoi.gateway.flink

import com.github.potamois.potamoi.gateway.flink.interact.EvictStrategy.DROP_TAIL

package object interact {

  // todo provide props via condition from hocon
  // FsiExecutor Config
  val props: ExecProps = baseProps("remote")

  lazy val baseProps = Map(
    "local" -> ExecProps.localEnv(rsCollectSt = DROP_TAIL -> 25),
    "remote" -> ExecProps.remoteEnv(remoteAddr = "hs.assad.site" -> 32241, rsCollectSt = DROP_TAIL -> 25)
  )

}
