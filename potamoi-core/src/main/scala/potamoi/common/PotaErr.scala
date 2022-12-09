package potamoi.common

import scala.util.control.NoStackTrace

trait PotaErr extends NoStackTrace

final case class FutureException[T](reason: T) extends NoStackTrace
