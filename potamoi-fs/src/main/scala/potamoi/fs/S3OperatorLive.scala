package potamoi.fs

import io.minio.{DownloadObjectArgs, GetObjectArgs, MinioClient, RemoveObjectArgs, UploadObjectArgs}
import io.minio.errors.ErrorResponseException
import potamoi.fs.S3Err.*
import potamoi.syntax.contra
import zio.{IO, UIO, ZIO}
import zio.ZIO.succeed

import java.io.File

class S3OperatorLive(s3Conf: S3Conf) extends S3Operator:

  private val minioClient = MinioClient
    .builder()
    .endpoint(s3Conf.endpoint)
    .credentials(s3Conf.accessKey, s3Conf.secretKey)
    .build()

  override def download(s3Path: String, targetPath: String): IO[IOErr | DownloadObjErr, File] =
    for {
      objectName <- extractObjectName(s3Path)
      _ <- lfs
        .ensureParentDir(targetPath)
        .mapError(e => IOErr(s"Fail to create parent directory of target file: $targetPath.", e.cause))
      _ <- ZIO
        .attemptBlockingInterrupt {
          minioClient.downloadObject(
            DownloadObjectArgs
              .builder()
              .bucket(s3Conf.bucket)
              .`object`(objectName)
              .filename(targetPath)
              .overwrite(true)
              .build()
          )
          new File(targetPath)
        }
        .mapError(DownloadObjErr(s3Path, objectName, s3Conf, _))
      file <- ZIO.succeed(File(targetPath))
    } yield file

  override def upload(filePath: String, s3Path: String, contentType: String): IO[FileNotFound | UploadObjErr, Unit] =
    for {
      _ <- ZIO
        .succeed(File(filePath).contra(f => f.exists() && f.isFile))
        .flatMap(ZIO.fail(FileNotFound(filePath)).unless(_))
      objectName <- extractObjectName(s3Path)
      _ <- ZIO
        .attemptBlockingInterrupt {
          minioClient.uploadObject(
            UploadObjectArgs
              .builder()
              .bucket(s3Conf.bucket)
              .`object`(objectName)
              .filename(filePath)
              .contentType(contentType)
              .build())
        }
        .mapError(UploadObjErr(s3Path, objectName, s3Conf, _))
    } yield ()

  override def remove(s3Path: String): IO[RemoveObjErr, Unit] =
    extractObjectName(s3Path).flatMap { objectName =>
      ZIO
        .attemptBlockingInterrupt {
          minioClient.removeObject(
            RemoveObjectArgs
              .builder()
              .bucket(s3Conf.bucket)
              .`object`(objectName)
              .build()
          )
        }
        .mapError(RemoveObjErr(s3Path, objectName, s3Conf, _))
    }

  override def exists(s3Path: String): IO[GetObjErr, Boolean] =
    extractObjectName(s3Path).flatMap { objectName =>
      ZIO
        .attemptBlockingInterrupt {
          minioClient.getObject(
            GetObjectArgs
              .builder()
              .bucket(s3Conf.bucket)
              .`object`(objectName)
              .build()
          )
        }
        .as(true)
        .catchSome { case e: ErrorResponseException if e.errorResponse().code() == "NoSuchKey" => succeed(false) }
        .mapError(GetObjErr(s3Path, objectName, s3Conf, _))
    }

  private def extractObjectName(s3Path: String): UIO[String] = ZIO.succeed {
    val path = paths.purePath(s3Path)
    val segs = path.split('/')
    if (segs(0) == s3Conf.bucket && s3Conf.accessStyle == S3AccessStyle.PathStyle) segs.drop(1).mkString("/")
    else path
  }
