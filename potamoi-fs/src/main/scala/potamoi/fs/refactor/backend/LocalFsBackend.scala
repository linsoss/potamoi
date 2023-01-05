package potamoi.fs.refactor.backend

import potamoi.fs.refactor.{paths, FsErr, LocalFsBackendConf, RemoteFsOperator}
import potamoi.fs.refactor.FsErr.{LfsErr, RfsErr}
import zio.{stream, IO, Scope, UIO, ZIO, ZLayer}
import zio.stream.ZStream

import java.io.File

/**
 * Local file system storage implementation for testing.
 */
object LocalFsBackend:

  val live = ZLayer {
    ZIO.service[LocalFsBackendConf].map(LocalFsBackend(_))
  }

class LocalFsBackend(conf: LocalFsBackendConf) extends RemoteFsOperator:

  override val name: String = "local"

  /**
   * Get the actual path to remote storage.
   */
  override def remotePath(path: String): UIO[String] = ZIO.succeed(s"$localStgDir/$path")
  private lazy val localStgDir                       = File(conf.dir).getAbsoluteFile

  /**
   * Upload file to remote storage.
   */
  override def upload(srcFile: File, targetPath: String): IO[FsErr, String] =
    for {
      target          <- purePath(targetPath)
      localTargetPath <- remotePath(target)
      _ <- ZIO
        .attemptBlocking {
          os.copy(from = os.Path(srcFile.getAbsolutePath), to = os.Path(localTargetPath), replaceExisting = true, createFolders = true)
        }
        .mapError(RfsErr(s"Fail to copy file in local fs: src=${srcFile.getAbsoluteFile}, target=$localTargetPath", _))
    } yield paths.withPotaSchema(target)

  /**
   * Download file from remote storage to local target path.
   */
  override def download(srcPath: String, targetPath: String): IO[FsErr, File] =
    for {
      src          <- purePath(srcPath)
      localSrcPath <- remotePath(src)
      _ <- ZIO
        .attemptBlocking {
          os.copy(from = os.Path(localSrcPath), to = os.Path(File(targetPath).getAbsolutePath), replaceExisting = true, createFolders = true)
        }
        .mapError(RfsErr(s"Fail to copy file in local fs: src=$localSrcPath, target=$targetPath", _))
    } yield File(targetPath)

  /**
   * Download file as ZStream.
   */
  override def downloadAsStream(srcPath: String): ZStream[Scope, FsErr, Byte] = {
    for {
      localSrcPath <- ZStream.fromZIO(purePath(srcPath).flatMap(remotePath))
      inputStream <- ZStream.acquireReleaseWith {
        ZIO.attemptBlockingInterrupt(os.read.inputStream(os.Path(localSrcPath)))
      } { fis => ZIO.attempt(fis.close()).ignore }
      stream <- ZStream.fromInputStream(inputStream)
    } yield stream
  }.mapError(RfsErr(s"Fail to get input stream bytes from local fs: ${remotePath(srcPath)}", _))

  /**
   * Remove file from remote storage.
   */
  override def remove(path: String): IO[FsErr, Unit] = for {
    localPath <- purePath(path).flatMap(remotePath)
    _ <- ZIO
      .attemptBlocking(os.remove(os.Path(localPath), checkExists = false))
      .mapError(RfsErr(s"Fail to remove file in local fs: path=$localPath", _))
  } yield ()

  /**
   * Determine whether file exists on remote storage.
   */
  override def exist(path: String): IO[FsErr, Boolean] = for {
    localPath <- purePath(path).flatMap(remotePath)
    exist <- ZIO
      .attemptBlocking(os.exists(os.Path(localPath)))
      .mapError(RfsErr(s"Fail to check file in local fs: path=$localPath", _))
  } yield exist
