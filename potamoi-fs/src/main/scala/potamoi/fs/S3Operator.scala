package potamoi.fs

import io.minio.*
import io.minio.errors.ErrorResponseException
import zio.{IO, UIO, ZIO, ZLayer}

import java.io.File

/**
 * S3 storage operator
 */
trait S3Operator:

  /**
   * Download object from s3 storage.
   */
  def download(s3Path: String, targetPath: String): IO[S3Err, File]

  /**
   * Upload file to s3 storage.
   */
  def upload(filePath: String, s3Path: String, contentType: String): IO[S3Err, Unit]

  /**
   * Delete s3 object
   */
  def remove(s3Path: String): IO[S3Err, Unit]

  /**
   * Tests whether the s3 object exists.
   */
  def exists(s3Path: String): IO[S3Err, Boolean]

object S3Operator:

  val live: ZLayer[S3Conf, Nothing, S3Operator] = ZLayer(ZIO.service[S3Conf].map(S3OperatorLive(_)))

  def remove(s3Path: String): ZIO[S3Operator, S3Err, Unit]                       = ZIO.serviceWithZIO[S3Operator](_.remove(s3Path))
  def exists(s3Path: String): ZIO[S3Operator, S3Err, Boolean]                    = ZIO.serviceWithZIO[S3Operator](_.exists(s3Path))
  def download(s3Path: String, targetPath: String): ZIO[S3Operator, S3Err, File] = ZIO.serviceWithZIO[S3Operator](_.download(s3Path, targetPath))
  def upload(filePath: String, s3Path: String, contentType: String): ZIO[S3Operator, S3Err, Unit] = ZIO.serviceWithZIO[S3Operator](
    _.upload(filePath, s3Path, contentType)
  )
