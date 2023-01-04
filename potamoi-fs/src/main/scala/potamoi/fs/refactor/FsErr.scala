package potamoi.fs.refactor

import potamoi.common.Err
import potamoi.fs.{FsErr, S3Conf}

/**
 * File system error.
 */
sealed abstract class FsErr(msg: String, cause: Throwable = null) extends Err(msg, cause)

object FsErr:
  case class LfsErr(msg: String, cause: Throwable = null) extends FsErr(msg, cause)
  case class RfsErr(msg: String, cause: Throwable = null) extends FsErr(msg, cause)
  case class UnSupportedSchema(path: String)                extends FsErr(s"Unsupported schema for path: $path")
