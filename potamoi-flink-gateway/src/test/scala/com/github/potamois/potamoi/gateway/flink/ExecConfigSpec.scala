package com.github.potamois.potamoi.gateway.flink

import com.github.potamois.potamoi.testkit.STSpec

class ExecConfigSpec extends STSpec {

  "ExecConfigSpec to EffectiveExecConfig" should {

    "create remote env correctly" in {
      val config = ExecConfig.remoteEnv(
        RemoteAddr("111.111.111.111", 8088),
        Map.empty,
        RsCollectStrategy(EvictStrategy.DROP_TAIL, 500)
      )
      config.toEffectiveExecConfig shouldBe EffectiveExecConfig(
        ExecConfig.DEFAULT_FLINK_CONFIG ++ Map(
          "rest.port" -> "8088",
          "rest.address" -> "111.111.111.111",
          "execution.target" -> "remote"),
        RsCollectStrategy(EvictStrategy.DROP_TAIL, 500)
      )
    }

    "create remote local env correctly" in {
      val config = ExecConfig.localEnv(
        Map.empty,
        RsCollectStrategy(EvictStrategy.DROP_TAIL, 500)
      )
      config.toEffectiveExecConfig shouldBe EffectiveExecConfig(
        ExecConfig.DEFAULT_FLINK_CONFIG ++ Map(
          "execution.target" -> "local"),
        RsCollectStrategy(EvictStrategy.DROP_TAIL, 500)
      )
    }

    "create env with custom flink configs" in {
      val config = ExecConfig.remoteEnv(
        RemoteAddr("111.111.111.111", 8088),
        Map(
          "pipeline.auto-generate-uids" -> "false",
          "pipeline.name" -> "test-pipeline",
          "execution.target" -> "local"
        ),
        RsCollectStrategy(EvictStrategy.DROP_TAIL, 500)
      )
      config.toEffectiveExecConfig shouldBe EffectiveExecConfig(
        ExecConfig.DEFAULT_FLINK_CONFIG ++ Map(
          "rest.port" -> "8088",
          "rest.address" -> "111.111.111.111",
          "execution.target" -> "local",
          "pipeline.auto-generate-uids" -> "false",
          "pipeline.name" -> "test-pipeline"),
        RsCollectStrategy(EvictStrategy.DROP_TAIL, 500)
      )
    }

  }

}
