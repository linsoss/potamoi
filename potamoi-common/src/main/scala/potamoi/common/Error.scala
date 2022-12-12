package potamoi.common

import scala.util.control.NoStackTrace

case object SilentErr extends NoStackTrace

final case class FutureException[T](reason: T) extends NoStackTrace
