package potamoi.fs

import potamoi.common.SilentErr

/**
 * S3 operation failure.
 */
sealed abstract class S3Err(msg: String, cause: Throwable = SilentErr) extends Exception(msg, cause)

object S3Err:
  case class IOErr(msg: String, cause: Throwable) extends S3Err(msg, cause)
  case class FileNotFound(path: String)           extends S3Err(s"File not found: $path")

  case class DownloadObjErr(s3Path: String, objName: String, s3Conf: S3Conf, cause: Throwable)
      extends S3ObjectOprErr("Download", s3Path, objName, s3Conf, cause)

  case class UploadObjErr(s3Path: String, objName: String, s3Conf: S3Conf, cause: Throwable)
      extends S3ObjectOprErr("Upload", s3Path, objName, s3Conf, cause)

  case class RemoveObjErr(s3Path: String, objName: String, s3Conf: S3Conf, cause: Throwable)
      extends S3ObjectOprErr("Delete", s3Path, objName, s3Conf, cause)

  case class GetObjErr(s3Path: String, objName: String, s3Conf: S3Conf, cause: Throwable)
      extends S3ObjectOprErr("Get", s3Path, objName, s3Conf, cause)

  sealed abstract class S3ObjectOprErr(
      opr: String,
      s3Path: String,
      objName: String,
      s3Conf: S3Conf,
      cause: Throwable)
      extends S3Err(
        s"$opr S3 object error: s3Path=$s3Path, objectName=$objName, bucket=${s3Conf.bucket}, endpoint=${s3Conf.endpoint}, accessStyle=${s3Conf.accessStyle}",
        cause)
