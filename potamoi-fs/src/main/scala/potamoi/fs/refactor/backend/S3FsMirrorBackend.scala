package potamoi.fs.refactor.backend

import io.minio.StatObjectArgs
import potamoi.errs.stackTraceString
import potamoi.fs.refactor.*
import potamoi.fs.refactor.FsErr.{LfsErr, RfsErr}
import potamoi.syntax.contra
import zio.{durationInt, IO, Scope, Task, UIO, ZIO, ZLayer}
import zio.cache.{Cache, Lookup}
import zio.direct.*
import zio.stream.ZStream
import zio.ZIO.{fail, logDebug, logInfo, logWarning, succeed}

import java.io.File
import scala.annotation.unused

/**
 * Hybrid s3/local fs storage backend, based on [[S3FsBackend]] and
 * using [[LocalFsBackend]] as cache.
 */
object S3FsMirrorBackend:

  val live: ZLayer[S3FsBackendConf, Nothing, S3FsMirrorBackend] = ZLayer {
    for {
      conf         <- ZIO.service[S3FsBackendConf]
      s3Backend    <- S3FsBackend.instance(conf)
      localBackend <- LocalFsBackend.instance(LocalFsBackendConf(conf.tmpDir))
      localCheckSumCache <- Cache.make[String, Any, LfsErr, String](
        capacity = 500,
        timeToLive = 114514.hours,
        lookup = Lookup(objectName => lfs.md5(File(localBackend.actualPath(objectName))))
      )
      _ <- logInfo(s"""Using S3FsMirrorBackend as RemoteFsOperator:
                      |   endpoint = ${conf.endpoint},
                      |   bucket = ${conf.bucket},
                      |   accessKey = ${conf.accessKey},
                      |   accessSecret = ***,
                      |   accessStyle = ${conf.accessStyle},
                      |   sslEnabled = ${conf.sslEnabled},
                      |   localMirrorDir = ${conf.tmpDir},
                      |   checkSumCacheCapacity = 500
                      |""".stripMargin)
    } yield S3FsMirrorBackend(s3Backend, localBackend, localCheckSumCache)
  }

/**
 * Default implementation.
 */
class S3FsMirrorBackend(
    s3Backend: S3FsBackend,
    localBackend: LocalFsBackend,
    localCheckSumCache: Cache[String, FsErr, String])
    extends RemoteFsOperator:

  override val name: String                     = "s3-mirror"
  override def actualPath(path: String): String = s3Backend.actualPath(path)

  /**
   * Upload file to remote storage.
   */
  override def upload(srcFile: File, targetPath: String): IO[FsErr, String] = s3Backend.upload(srcFile, targetPath)

  /**
   * Download file from remote storage to local target path.
   */
  override def download(srcPath: String, targetPath: String): IO[FsErr, File] = defer {
    val useMirror = canUseMirror(s3Backend.objectNameOf(srcPath)).run
    if useMirror then
      logDebug(s"Hit local mirror file for $srcPath").run
      localBackend.download(srcPath, targetPath).run
    else
      s3Backend
        .download(srcPath, targetPath)
        .tap { outFile => localBackend.upload(outFile, srcPath).ignore }
        .run
  }

  /**
   * Download file from remote storage to local temporary directory.
   */
  override def download(srcPath: String): IO[FsErr, File] = defer {
    val useMirror = canUseMirror(s3Backend.objectNameOf(srcPath)).run
    if useMirror then
      logDebug(s"Hit local mirror file for $srcPath").run
      succeed(File(localBackend.actualPath(srcPath))).run
    else s3Backend.download(srcPath).run
  }

  /**
   * Download file as ZStream.
   */
  override def downloadAsStream(srcPath: String): ZStream[Scope, FsErr, Byte] =
    for {
      useMirror <- ZStream.fromZIO(canUseMirror(s3Backend.objectNameOf(srcPath)))
      stream <- {
        if useMirror then
          ZStream.logDebug(s"Hit local mirror file for $srcPath") *>
          localBackend.downloadAsStream(srcPath)
        else s3Backend.downloadAsStream(srcPath)
      }
    } yield stream

  /**
   * Remove file from remote storage.
   */
  override def remove(path: String): IO[FsErr, Unit] = {
    s3Backend.remove(path) <*
    localCheckSumCache.invalidate(s3Backend.objectNameOf(path)) <*
    localBackend.remove(path).ignore
  }

  /**
   * Determine whether file exists on remote storage.
   */
  override def exist(path: String): IO[FsErr, Boolean] = s3Backend.exist(path)

  /**
   * Compare the checksum values of the s3 object and the corresponding local storage to
   * determine whether to use the local file mirror.
   */
  private def canUseMirror(objectName: String): UIO[Boolean] = {
    defer {
      val localExists = localBackend.exist(objectName).run
      if !localExists then false
      else
        val (s3CheckSum, localCheckSum) = (retrieveS3ObjectETag(objectName) <&> localCheckSumCache.get(objectName)).run
        s3CheckSum == localCheckSum
    }
  } catchAll { case e: Throwable =>
    logWarning(s"Fail to retrieve determine download source, use s3 source directly: s3Object=$objectName, cause=${e.getMessage}") *>
    succeed(false)
  }

  private def retrieveS3ObjectETag(objectName: String): Task[String] = ZIO.attemptBlocking {
    s3Backend.minioClient
      .statObject(StatObjectArgs.builder.bucket(s3Backend.conf.bucket).`object`(objectName).build)
      .etag()
  }

end S3FsMirrorBackend
