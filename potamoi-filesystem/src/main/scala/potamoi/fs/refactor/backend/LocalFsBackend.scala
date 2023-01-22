package potamoi.fs.refactor.backend

import potamoi.fs.refactor.*
import potamoi.fs.refactor.FsErr.{LfsErr, RfsErr}
import potamoi.syntax.contra
import zio.{stream, IO, Scope, UIO, ZIO, ZLayer}
import zio.stream.ZStream
import zio.ZIO.{fail, log, logDebug, logInfo, succeed, unit}
import zio.direct.*

import java.io.File

/**
 * Local file system storage implementation for testing.
 */
object LocalFsBackend:
  val live = ZLayer {
    for {
      conf    <- ZIO.service[LocalFsBackendConf]
      backend <- make(conf)
      _       <- hintLog(conf)
    } yield backend
  }

  def hintLog(conf: LocalFsBackendConf): UIO[Unit] = {
    logInfo(s"""Using LocalFsBackend as RemoteFsOperator: storeDir = ${conf.dir}""")
  }

  def make(conf: LocalFsBackendConf): UIO[LocalFsBackend] = {
    succeed(LocalFsBackend(conf))
  }

/**
 * Default implementation.
 */
case class LocalFsBackend(conf: LocalFsBackendConf) extends RemoteFsOperator:

  override val name: String                     = "local"
  private lazy val localStgDir                  = File(conf.dir).getAbsoluteFile
  override def actualPath(path: String): String = paths.purePath(path).contra(p => s"$localStgDir/$p")

  /**
   * Upload file to remote storage.
   */
  override def upload(srcFile: File, targetPath: String): IO[FsErr, String] = {
    val target         = paths.purePath(targetPath)
    val localStorePath = actualPath(target)
    ZIO
      .attemptBlocking(
        os.copy(
          from = os.Path(srcFile.getAbsolutePath),
          to = os.Path(localStorePath),
          replaceExisting = true,
          createFolders = true
        ))
      .mapBoth(
        RfsErr(s"Fail to copy file in local fs: src=${srcFile.getAbsoluteFile}, target=$localStorePath", _),
        _ => paths.withPotaSchema(target)
      )
  }

  /**
   * Download file from remote storage to local temporary directory.
   */
  override def download(srcPath: String): IO[FsErr, File] = defer {
    val localStorePath = actualPath(srcPath)
    val exists         = lfs.existFile(localStorePath).run
    if exists then succeed(File(localStorePath)).run
    else fail(RfsErr(s"File not found in local fs: $localStorePath")).run
  }

  /**
   * Download file from remote storage to local target path.
   */
  override def download(srcPath: String, targetPath: String): IO[FsErr, File] = {
    val localStorePath = actualPath(srcPath)
    ZIO
      .attemptBlocking(
        os.copy(
          from = os.Path(localStorePath),
          to = os.Path(File(targetPath).getAbsolutePath),
          replaceExisting = true,
          createFolders = true
        ))
      .mapBoth(
        RfsErr(s"Fail to copy file in local fs: src=$localStorePath, target=$targetPath", _),
        _ => File(targetPath)
      )
  }

  /**
   * Download file as ZStream.
   */
  override def downloadAsStream(srcPath: String): ZStream[Scope, FsErr, Byte] = {
    for {
      localSrcPath <- ZStream.from(actualPath(srcPath))
      inputStream  <- ZStream
                        .acquireReleaseWith {
                          ZIO.attemptBlockingInterrupt(os.read.inputStream(os.Path(localSrcPath)))
                        } { fis =>
                          ZIO.attempt(fis.close()).ignore
                        }
      stream       <- ZStream.fromInputStream(inputStream)
    } yield stream
  }.mapError(RfsErr(s"Fail to get input stream bytes from local fs: ${actualPath(srcPath)}", _))

  /**
   * Remove file from remote storage.
   */
  override def remove(path: String): IO[FsErr, Unit] = {
    val localStorePath = actualPath(path)
    ZIO
      .attemptBlocking(os.remove(os.Path(localStorePath), checkExists = false))
      .mapError(RfsErr(s"Fail to remove file in local fs: path=$localStorePath", _))
      .unit
  }

  /**
   * Determine whether file exists on remote storage.
   */
  override def exist(path: String): IO[FsErr, Boolean] = {
    val localStorePath = actualPath(path)
    lfs.existFile(localStorePath)
  }
