package potamoi.common

import akka.util.Timeout
import zio.Duration as ZIODuration

import java.time.Duration as JavaDuration
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration as ScalaDuration, FiniteDuration, NANOSECONDS, TimeUnit}

/**
 * Time, Duration extension.
 */
object TimeExtension:

  given Conversion[FiniteDuration, JavaDuration] = duration => JavaDuration.ofNanos(duration.toNanos)

  given Conversion[ScalaDuration, ZIODuration] = {
    case ScalaDuration.Inf  => ZIODuration.Infinity
    case ScalaDuration.Zero => ZIODuration.Zero
    case d                  => ZIODuration.fromNanos(d.toNanos)
  }

  given Conversion[ZIODuration, ScalaDuration] = {
    case ZIODuration.Infinity => ScalaDuration.Inf
    case ZIODuration.Zero     => ScalaDuration.Zero
    case d                    => ScalaDuration.fromNanos(d.toNanos)
  }

  given Conversion[ScalaDuration, Timeout] = {
    case d: FiniteDuration => Timeout(d)
    case _                 => Timeout(FiniteDuration(102400, TimeUnit.DAYS))
  }

  given Conversion[ZIODuration, Timeout] = {
    (given_Conversion_ZIODuration_ScalaDuration andThen given_Conversion_ScalaDuration_Timeout).apply(_)
  }

  given Conversion[ScalaDuration, FiniteDuration] = {
    case d: FiniteDuration => d
    case _                 => FiniteDuration(102400, TimeUnit.DAYS)
  }
