package potamoi.fs

import potamoi.common.Err

/**
 * S3 operation failure.
 */
//sealed abstract class S3Err(msg: String, cause: Throwable = null) extends PotaErr(msg, cause)
sealed abstract class S3Err(msg: String, cause: Throwable = null) extends Err(msg, cause)

object S3Err:
  case class IOErr(msg: String, cause: Throwable) extends S3Err(msg, cause)
  case class FileNotFound(path: String)           extends S3Err(s"File not found: path=$path")

  case class DownloadObjOperationErr(s3Path: String, objName: String, s3Conf: S3Conf, cause: Throwable)
      extends S3ObjectOperationErr("Download", s3Path, objName, s3Conf, cause)

  case class UploadObjOperationErr(s3Path: String, objName: String, s3Conf: S3Conf, cause: Throwable)
      extends S3ObjectOperationErr("Upload", s3Path, objName, s3Conf, cause)

  case class RemoveObjOperationErr(s3Path: String, objName: String, s3Conf: S3Conf, cause: Throwable)
      extends S3ObjectOperationErr("Delete", s3Path, objName, s3Conf, cause)

  case class GetObjOperationErr(s3Path: String, objName: String, s3Conf: S3Conf, cause: Throwable)
      extends S3ObjectOperationErr("Get", s3Path, objName, s3Conf, cause)

  sealed abstract class S3ObjectOperationErr(
      opr: String,
      s3Path: String,
      objName: String,
      s3Conf: S3Conf,
      cause: Throwable)
      extends S3Err(
        s"$opr S3 object error: s3Path=$s3Path, objectName=$objName, bucket=${s3Conf.bucket}, endpoint=${s3Conf.endpoint}, accessStyle=${s3Conf.accessStyle}",
        cause)
