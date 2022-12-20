package potamoi

import potamoi.common.Syntax.toPrettyString
import zio.{durationInt, IO, ZIO}
import zio.Schedule.spaced

package object flink:

  extension [E, A](io: IO[E, A]) {
    def watch: IO[E, Unit]                       = io.debug.repeat(spaced(1.seconds)).unit
    def watchTag(tag: String): IO[E, Unit]       = io.debug(tag).repeat(spaced(1.seconds)).unit
    def watchPretty: IO[E, Unit]                 = io.map(toPrettyString(_)).debug.repeat(spaced(1.seconds)).unit
    def watchPrettyTag(tag: String): IO[E, Unit] = io.map(toPrettyString(_)).debug(tag).repeat(spaced(1.seconds)).unit
  }
