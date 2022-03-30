package com.github.potamois.potamoi.akka

import com.typesafe.config.{Config, ConfigFactory}

package object testkit {

  // default akka config for scalatest
  val defaultConfig: Config = ConfigFactory.load("application-test")

}
