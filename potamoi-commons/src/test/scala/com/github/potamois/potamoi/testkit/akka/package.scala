package com.github.potamois.potamoi.testkit

import com.typesafe.config.{Config, ConfigFactory}

package object akka {

  // default akka config for scalatest
  val defaultConfig: Config = ConfigFactory.load("application-test")

}
