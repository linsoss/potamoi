package potamoi.fs

import potamoi.fs.LfsErr.IOErr
import potamoi.syntax.contra
import zio.{IO, ZIO}

import java.io.File

/**
 * Local file system operator.
 */
val lfs = LfsOperator

object LfsOperator {

  /**
   * Tests whether the file denoted by this abstract pathname exists.
   */
  def fileExists(path: String): IO[IOErr, Boolean] =
    ZIO
      .attempt(new File(path).contra(f => f.exists() && f.isFile))
      .mapError(IOErr(s"Unable to access file: $path", _))

  /**
   * Delete file or directory recursively of given path.
   */
  def rm(path: String): IO[IOErr, Unit] =
    ZIO
      .attemptBlocking(os.remove.all(os.Path(new File(path).getAbsolutePath)))
      .mapError(IOErr(s"Unable to remove file: $path", _))

  /**
   * Write content to file.
   */
  def write(path: String, content: String): IO[IOErr, Unit] =
    (ensureParentDir(path) *> ZIO.writeFile(path, content)).mapError {
      case fail: IOErr      => fail
      case cause: Throwable => IOErr(s"Unable to write file: $path", cause)
    }

  /**
   * Ensure the parent directory of given path would be created.
   */
  def ensureParentDir(path: String): IO[IOErr, Unit] = ensureParentDir(File(path))

  def ensureParentDir(file: File): IO[IOErr, Unit] =
    ZIO
      .attempt {
        val parent = file.toPath.getParent
        if (parent != null) parent.toFile.mkdirs()
      }
      .unit
      .mapError(IOErr(s"Unable to ensure parent directory of file: ${file.getAbsolutePath}", _))
}
