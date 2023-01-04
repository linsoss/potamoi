package potamoi.fs

import potamoi.common.Err

/**
 * File system error.
 */
sealed abstract class FsErr(msg: String, cause: Throwable = null) extends Err(msg, cause)

/**
 * Local file system error.
 */
sealed abstract class LocalFsErr(msg: String, cause: Throwable = null) extends FsErr(msg, cause)

object LocalFsErr:
  case class FileNotFound(path: String)           extends LocalFsErr(s"File not found: path=$path")
  case class IOErr(msg: String, cause: Throwable) extends LocalFsErr(msg, cause)

/**
 * S3 operation failure.
 */
sealed abstract class S3Err(msg: String, cause: Throwable = null) extends FsErr(msg, cause)

object S3Err:
  case class GetObjErr(s3Path: String, objName: String, s3Conf: S3Conf, cause: Throwable) extends S3ObjectErr("Get", s3Path, objName, s3Conf, cause)
  case class DownloadObjErr(s3Path: String, objName: String, s3Conf: S3Conf, cause: Throwable)
      extends S3ObjectErr("Download", s3Path, objName, s3Conf, cause)
  case class UploadObjErr(s3Path: String, objName: String, s3Conf: S3Conf, cause: Throwable)
      extends S3ObjectErr("Upload", s3Path, objName, s3Conf, cause)
  case class RemoveObjErr(s3Path: String, objName: String, s3Conf: S3Conf, cause: Throwable)
      extends S3ObjectErr("Delete", s3Path, objName, s3Conf, cause)

  sealed abstract class S3ObjectErr(
      opr: String,
      s3Path: String,
      objName: String,
      s3Conf: S3Conf,
      cause: Throwable)
      extends S3Err(
        s"$opr S3 object error: s3Path=$s3Path, objectName=$objName, bucket=${s3Conf.bucket}, endpoint=${s3Conf.endpoint}, accessStyle=${s3Conf.accessStyle}",
        cause)
