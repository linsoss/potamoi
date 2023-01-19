package potamoi.common

import potamoi.syntax.toPrettyString
import zio.stream.ZStream
import zio.{durationInt, Duration, IO, Schedule}

object DebugKit:

  extension [E, A](io: IO[E, A]) {
    def watch: IO[E, Unit]                       = watchStream().debug.runDrain
    def watchTag(tag: String): IO[E, Unit]       = watchStream().debug(tag).runDrain
    def watchPretty: IO[E, Unit]                 = watchStream().map(toPrettyString).debug.runDrain
    def watchPrettyTag(tag: String): IO[E, Unit] = watchStream().map(toPrettyString).debug(tag).runDrain

    inline def watchStream(spaced: Duration = 500.millis) =
      ZStream
        .fromZIO(io)
        .repeat(Schedule.spaced(500.millis))
        .zipWithPrevious
        .filter { case (prev, curr) => !prev.contains(curr) }
        .map(_._2)
  }
