package potamoi.fs.refactor.backend

import io.minio.*
import io.minio.errors.ErrorResponseException
import io.minio.messages.Tags
import potamoi.common.Err
import potamoi.fs.refactor.*
import potamoi.fs.refactor.FsErr.{LfsErr, RfsErr}
import potamoi.fs.S3Err.UploadObjErr
import potamoi.fs.refactor.backend.S3FsBackend.instance
import potamoi.syntax.contra
import zio.{durationInt, stream, IO, Scope, Task, UIO, ZIO, ZLayer}
import zio.ZIO.{logInfo, succeed, unit}
import zio.cache.{Cache, Lookup}
import zio.stream.{Stream, ZStream}

import java.io.File
import scala.jdk.CollectionConverters.*

/**
 * S3 storage backend for potamoi system.
 */
object S3FsBackend:

  val live: ZLayer[S3FsBackendConf, Nothing, S3FsBackend] = ZLayer {
    for {
      conf <- ZIO.service[S3FsBackendConf]
      inst <- instance(conf)
      _ <- logInfo(s"""Using S3FsBackendConf as RemoteFsOperator:
                      |   endpoint = ${conf.endpoint},
                      |   bucket = ${conf.bucket},
                      |   accessKey = ${conf.accessKey},
                      |   accessSecret = ***,
                      |   accessStyle = ${conf.accessStyle},
                      |   sslEnabled = ${conf.sslEnabled},
                      |   tmpDir = ${conf.tmpDir},
                      |""".stripMargin)
    } yield inst
  }

  def instance(conf: S3FsBackendConf): ZIO[Any, Nothing, S3FsBackend] =
    for {
      _ <- unit
      minioClient = MinioClient.builder
        .endpoint(conf.endpoint)
        .credentials(conf.accessKey, conf.secretKey)
        .build
    } yield S3FsBackend(minioClient, conf)

end S3FsBackend

/**
 * Default implementation.
 */
case class S3FsBackend(minioClient: MinioClient, conf: S3FsBackendConf) extends RemoteFsOperator:

  override val name: String                     = "s3"
  override def actualPath(path: String): String = objectNameOf(path).contra(p => s"s3://${conf.bucket}/$p")
  def objectNameOf(potaPath: String): String    = paths.purePath(potaPath)

  /**
   * Upload file to remote storage.
   */
  override def upload(srcFile: File, targetPath: String): IO[FsErr, String] =
    for {
      _ <- unit
      objectName = objectNameOf(targetPath)
      _ <- ZIO
        .succeed(srcFile.exists() && srcFile.isFile)
        .flatMap {
          ZIO.fail(LfsErr(s"File not found: ${srcFile.getAbsoluteFile}")).unless(_)
        }
      srcPath = srcFile.getAbsolutePath
      _ <- ZIO
        .attemptBlockingInterrupt {
          minioClient.uploadObject(
            UploadObjectArgs.builder
              .bucket(conf.bucket)
              .`object`(objectName)
              .filename(srcPath)
              .contentType(paths.detectMimeType(srcPath))
              .tags(Map("a" -> "b").asJava)
              .build)
        }
        .mapError {
          RfsErr(s"Fail to upload file to S3: srcFile=$srcPath, s3Path=${actualPath(objectName)}", _)
        }
    } yield paths.withPotaSchema(objectName)

  /**
   * Download file from remote storage to local temporary directory.
   */
  override def download(srcPath: String): IO[FsErr, File] =
    download(srcPath, s"${conf.tmpDir}/${objectNameOf(srcPath)}")

  /**
   * Download file from remote storage to local target path.
   */
  override def download(srcPath: String, targetPath: String): IO[FsErr, File] =
    for {
      _ <- unit
      objectName = objectNameOf(srcPath)
      _ <- lfs
        .ensureParentDir(targetPath)
        .mapError { e =>
          LfsErr(s"Fail to create parent directory of target file: $targetPath.", e.cause)
        }
      _ <- ZIO
        .attemptBlockingInterrupt {
          minioClient.downloadObject(
            DownloadObjectArgs.builder.bucket(conf.bucket).`object`(objectName).filename(targetPath).overwrite(true).build
          )
        }
        .mapError {
          RfsErr(s"Fail to download file from S3: s3Path=${actualPath(srcPath)}, targetPath=$targetPath", _)
        }
    } yield File(targetPath)

  /**
   * Download file as ZStream.
   */
  override def downloadAsStream(srcPath: String): ZStream[Scope, FsErr, Byte] = {
    for {
      objectName <- ZStream.from(objectNameOf(srcPath))
      inputStream <- ZStream.acquireReleaseWith {
        ZIO.attemptBlockingInterrupt {
          minioClient.getObject(GetObjectArgs.builder.bucket(conf.bucket).`object`(objectName).build)
        }
      } { fis => ZIO.attempt(fis.close()).ignore }
      stream <- ZStream.fromInputStream(inputStream)
    } yield stream
  }.mapError(e => RfsErr(s"Fail to get object stream from S3: ${actualPath(srcPath)}", e))

  /**
   * Remove file from remote storage.
   */
  override def remove(path: String): IO[FsErr, Unit] = {
    val objectName = objectNameOf(path)
    ZIO
      .attemptBlockingInterrupt {
        minioClient.removeObject(RemoveObjectArgs.builder.bucket(conf.bucket).`object`(objectName).build)
      }
      .mapError {
        RfsErr(s"Fail to remove object on s3: s3Path=${actualPath(path)}", _)
      }
  }

  /**
   * Determine whether file exists on remote storage.
   */
  override def exist(path: String): IO[FsErr, Boolean] =
    for {
      _ <- unit
      objectName = objectNameOf(path)
      exists <- ZIO
        .attemptBlockingInterrupt { minioClient.statObject(StatObjectArgs.builder.bucket(conf.bucket).`object`(objectName).build) }
        .as(true)
        .catchSome { case e: ErrorResponseException if e.errorResponse().code() == "NoSuchKey" => succeed(false) }
        .mapError(RfsErr(s"Fail to get object from S3: s3Path=${actualPath(path)}", _))
    } yield exists
