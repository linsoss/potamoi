package potamoi.common

import zio.{Duration => ZioDuration}
import java.time.{Duration => JavaDuration}
import scala.concurrent.duration.{Duration => ScalaDuration, FiniteDuration, NANOSECONDS}
import scala.concurrent.duration.{FiniteDuration, NANOSECONDS}

/**
 * Time, Duration extension.
 */
object TimeExtension {

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

}
