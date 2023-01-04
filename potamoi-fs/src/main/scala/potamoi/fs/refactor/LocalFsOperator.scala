package potamoi.fs.refactor

import potamoi.fs.refactor.FsErr.LfsErr
import potamoi.syntax.contra
import zio.{IO, ZIO}

import java.io.File

/**
 * Local file system operator.
 */
val lfs = LocalFsOperator

object LocalFsOperator {

  /**
   * Tests whether the file denoted by this abstract pathname exists.
   */
  def existFile(path: String): IO[LfsErr, Boolean] =
    ZIO
      .attempt(new File(path).contra(f => f.exists() && f.isFile))
      .mapError(LfsErr(s"Unable to access file: $path", _))

  /**
   * Delete file or directory recursively of given path.
   */
  def rm(path: String): IO[LfsErr, Unit] =
    ZIO
      .attemptBlocking(os.remove.all(os.Path(new File(path).getAbsolutePath)))
      .mapError(LfsErr(s"Unable to remove file: $path", _))

  /**
   * Write content to file.
   */
  def write(path: String, content: String): IO[LfsErr, Unit] =
    (ensureParentDir(path) *> ZIO.writeFile(path, content)).mapError {
      case fail: LfsErr     => fail
      case cause: Throwable => LfsErr(s"Unable to write file: $path", cause)
    }

  /**
   * Ensure the parent directory of given path would be created.
   */
  def ensureParentDir(path: String): IO[LfsErr, Unit] = ensureParentDir(File(path))
  def ensureParentDir(file: File): IO[LfsErr, Unit] =
    ZIO
      .attempt {
        val parent = file.toPath.getParent
        if (parent != null) parent.toFile.mkdirs()
      }
      .unit
      .mapError(LfsErr(s"Unable to ensure parent directory of file: ${file.getAbsolutePath}", _))

}
