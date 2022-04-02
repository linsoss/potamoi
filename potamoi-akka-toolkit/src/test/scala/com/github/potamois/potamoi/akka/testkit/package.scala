package com.github.potamois.potamoi.akka

import com.github.potamois.potamoi.commons.PotaConfig
import com.typesafe.config.Config

package object testkit {

  // default akka config for scalatest
  val defaultConfig: Config = PotaConfig.root

}
