package potamoi.fs

import potamoi.PotaErr

/**
 * File system error.
 */
sealed trait FsErr extends PotaErr

object FsErr:
  case class LfsErr(msg: String, cause: Throwable = null) extends FsErr
  case class RfsErr(msg: String, cause: Throwable = null) extends FsErr
  case class UnSupportedSchema(path: String)              extends FsErr
