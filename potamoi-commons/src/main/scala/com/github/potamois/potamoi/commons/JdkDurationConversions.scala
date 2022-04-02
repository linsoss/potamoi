package com.github.potamois.potamoi.commons

import java.time.{Duration => JdkDuration}
import scala.concurrent.duration.Duration
import scala.language.implicitConversions

/**
 * Implicit conversion between scala [[scala.concurrent.duration.Duration]] and java [[java.time.Duration]].
 *
 * @author Al-assad
 */
object JdkDurationConversions {

  implicit def asScala(jdkDuration: JdkDuration): Duration = Duration.fromNanos(jdkDuration.toNanos)

  implicit def asJava(scalaDuration: Duration): JdkDuration = JdkDuration.ofNanos(scalaDuration.toNanos)

}

