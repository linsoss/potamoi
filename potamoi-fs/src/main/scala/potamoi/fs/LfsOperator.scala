package potamoi.fs

import potamoi.syntax.contra
import zio.{IO, ZIO}

import java.io.File

/**
 * Local file system operator.
 */
val lfs = LfsOperator

case class LfsIOErr(cause: Throwable) extends Exception(cause)

object LfsOperator:

  /**
   * Tests whether the file denoted by this abstract pathname exists.
   */
  def fileExists(path: String): IO[LfsIOErr, Boolean] =
    ZIO
      .attempt(new File(path).contra(f => f.exists() && f.isFile))
      .mapError(LfsIOErr.apply)

  /**
   * Delete file or directory recursively of given path.
   */
  def rm(path: String): IO[LfsIOErr, Unit] =
    ZIO
      .attemptBlocking(os.remove.all(os.Path(new File(path).getAbsolutePath)))
      .mapError(LfsIOErr.apply)

  /**
   * Write content to file.
   */
  def write(path: String, content: String): IO[LfsIOErr, Unit] =
    (ensureParentDir(path) *> ZIO.writeFile(path, content)).mapError {
      case fail: LfsIOErr   => fail
      case cause: Throwable => LfsIOErr(cause)
    }

  /**
   * Ensure the parent directory of given path would be created.
   */
  def ensureParentDir(path: String): IO[LfsIOErr, Unit] = ensureParentDir(File(path))

  def ensureParentDir(file: File): IO[LfsIOErr, Unit] =
    ZIO
      .attempt {
        val parent = file.toPath.getParent
        if (parent != null) parent.toFile.mkdirs()
      }
      .unit
      .mapError(LfsIOErr.apply)
