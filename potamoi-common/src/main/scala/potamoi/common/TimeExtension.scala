package potamoi.common

import zio.Duration as ZioDuration

import java.time.Duration as JavaDuration
import scala.concurrent.duration.{Duration as ScalaDuration, FiniteDuration, NANOSECONDS}

/**
 * Time, Duration extension.
 */
object TimeExtension:

  given Conversion[FiniteDuration, JavaDuration] = duration => JavaDuration.ofNanos(duration.toNanos)

  given Conversion[ScalaDuration, ZioDuration] = {
    case ScalaDuration.Inf  => ZioDuration.Infinity
    case ScalaDuration.Zero => ZioDuration.Zero
    case d                  => ZioDuration.fromNanos(d.toNanos)
  }

  given Conversion[ZioDuration, ScalaDuration] = {
    case ZioDuration.Infinity => ScalaDuration.Inf
    case ZioDuration.Zero     => ScalaDuration.Zero
    case d                    => ScalaDuration.fromNanos(d.toNanos)
  }
