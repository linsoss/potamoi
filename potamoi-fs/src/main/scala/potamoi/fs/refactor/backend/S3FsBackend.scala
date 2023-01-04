package potamoi.fs.refactor.backend

import io.minio.*
import io.minio.errors.ErrorResponseException
import potamoi.fs.refactor.*
import potamoi.fs.refactor.FsErr.{LfsErr, RfsErr}
import potamoi.fs.S3Err.UploadObjErr
import potamoi.syntax.contra
import zio.{stream, IO, UIO, ZIO, ZLayer}
import zio.ZIO.succeed
import zio.stream.{Stream, ZStream}

import java.io.File

/**
 * S3 storage backend for potamoi system.
 */
object S3FsBackend:

  val live: ZLayer[S3FsBackendConf, Nothing, S3FsBackend] = ZLayer {
    for {
      conf <- ZIO.service[S3FsBackendConf]
      minioClient = MinioClient.builder
        .endpoint(conf.endpoint)
        .credentials(conf.accessKey, conf.secretKey)
        .build
    } yield S3FsBackend(minioClient, conf)
  }

/**
 * Default implementation.
 */
class S3FsBackend(minioClient: MinioClient, conf: S3FsBackendConf) extends RemoteFsOperator:

  override val name: String = "s3"

  /**
   * Get the actual path to remote storage.
   */
  override def remotePath(path: String): UIO[String] = ZIO.succeed(s"s3://${conf.bucket}/$path")

  /**
   * Upload file to remote storage.
   */
  override def upload(srcFile: File, targetPath: String): IO[FsErr, String] =
    for {
      objectName <- purePath(targetPath)
      _ <- ZIO
        .succeed(srcFile.exists() && srcFile.isFile)
        .flatMap {
          ZIO.fail(LfsErr(s"File not found: ${srcFile.getAbsoluteFile}")).unless(_)
        }
      srcPath = srcFile.getAbsolutePath
      _ <- ZIO
        .attemptBlockingInterrupt {
          minioClient.uploadObject(
            UploadObjectArgs.builder.bucket(conf.bucket).`object`(objectName).filename(srcPath).contentType(paths.detectMimeType(srcPath)).build
          )
        }
        .mapError(
          RfsErr(s"Fail to upload file to S3: srcFile=$srcPath, s3Path=${remotePath(objectName)}", _)
        )
    } yield paths.withPotaSchema(objectName)

  /**
   * Download file from remote storage to local target path.
   */
  override def download(srcPath: String, targetPath: String): IO[FsErr, File] = for {
    objectName <- purePath(srcPath)
    _ <- lfs
      .ensureParentDir(targetPath)
      .mapError { e =>
        LfsErr(s"Fail to create parent directory of target file: $targetPath.", e.cause)
      }
    _ <- ZIO
      .attemptBlockingInterrupt {
        minioClient.downloadObject(DownloadObjectArgs.builder.bucket(conf.bucket).`object`(objectName).filename(targetPath).overwrite(true).build)
      }
      .mapError(
        RfsErr(s"Fail to upload file from S3: s3Path=${remotePath(srcPath)}, targetPath=$targetPath", _)
      )
  } yield File(targetPath)

  /**
   * Download file as ZStream.
   */
  override def downloadAsStream(srcPath: String): Stream[FsErr, Byte] = {
    for {
      objectName <- ZStream.fromZIO(purePath(srcPath))
      inputStream <- ZStream.fromZIO {
        ZIO
          .attemptBlockingInterrupt { minioClient.getObject(GetObjectArgs.builder.bucket(conf.bucket).`object`(objectName).build) }
          .mapError(RfsErr(s"Fail to get object stream from S3: $srcPath", _))
      }
      stream <- ZStream.fromInputStream(inputStream).mapError(e => RfsErr(s"Fail to receive object stream from S3: $srcPath", e))
    } yield stream
  }

  /**
   * Remove file from remote storage.
   */
  override def remove(path: String): IO[FsErr, Unit] =
    for {
      objectName <- purePath(path)
      _ <- ZIO
        .attemptBlockingInterrupt { minioClient.removeObject(RemoveObjectArgs.builder.bucket(conf.bucket).`object`(objectName).build) }
        .mapError(RfsErr(s"Fail to remove object on s3: s3Path=${remotePath(path)}", _))
    } yield ()

  /**
   * Determine whether file exists on remote storage.
   */
  override def exist(path: String): IO[FsErr, Boolean] =
    for {
      objectName <- purePath(path)
      exists <- ZIO
        .attemptBlockingInterrupt { minioClient.statObject(StatObjectArgs.builder.bucket(conf.bucket).`object`(objectName).build) }
        .as(true)
        .catchSome { case e: ErrorResponseException if e.errorResponse().code() == "NoSuchKey" => succeed(false) }
        .mapError(RfsErr(s"Fail to get object from S3: s3Path=${remotePath(path)}", _))
    } yield exists
