package com.github.potamois.potamoi.testkit

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/**
 * Basic standard ScalaTest testkit.
 *
 * @author Al-assad
 */
trait STSpec extends AnyWordSpec
  with Matchers
  with BeforeAndAfterEach
  with BeforeAndAfterAll
