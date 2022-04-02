package com.github.potamois.potamoi.commons

import java.time.{Duration => JdkDuration}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{DAYS, Duration, FiniteDuration, HOURS, MICROSECONDS, MILLISECONDS, MINUTES, NANOSECONDS, SECONDS}
import scala.language.implicitConversions

/**
 * Implicit conversion between scala [[scala.concurrent.duration.Duration]] and java [[java.time.Duration]].
 *
 * @author Al-assad
 */
object JdkDurationConversions {

  implicit def toScala(jdkDuration: JdkDuration): FiniteDuration = toScala(jdkDuration, TimeUnit.NANOSECONDS)

  implicit def toScala(jdkDuration: JdkDuration, unit: TimeUnit): FiniteDuration = unit match {
    case TimeUnit.DAYS => FiniteDuration(jdkDuration.toDays, DAYS)
    case TimeUnit.HOURS => FiniteDuration(jdkDuration.toHours, HOURS)
    case TimeUnit.MINUTES => FiniteDuration(jdkDuration.toMinutes, MINUTES)
    case TimeUnit.SECONDS => FiniteDuration(jdkDuration.getSeconds, SECONDS)
    case TimeUnit.MILLISECONDS => FiniteDuration(jdkDuration.toMillis, MILLISECONDS)
    case TimeUnit.MICROSECONDS => FiniteDuration(jdkDuration.toNanos, MICROSECONDS)
    case TimeUnit.NANOSECONDS => FiniteDuration(jdkDuration.toNanos, NANOSECONDS)
  }

  implicit def toJava(scalaDuration: Duration): JdkDuration = JdkDuration.ofNanos(scalaDuration.toNanos)

  implicit class JavaDurationImplicit(jdkDuration: JdkDuration) {
    def asScala(unit: TimeUnit = NANOSECONDS): FiniteDuration = toScala(jdkDuration, unit)
  }

}

