package potamoi.fs.refactor

import potamoi.fs.refactor.FsErr
import zio.{IO, UIO, ZIO}
import zio.stream.Stream

import java.io.File
import java.net.{URI, URL}
import java.nio.file.Path

/**
 * Potamoi remote fs storage trait, Used to provide a unified storage
 * operation interface for S3, HDFS, FTP and other backends.
 */
trait RemoteFsOperator:

  /**
   * backend name.
   */
  val name: String

  /**
   * Get the actual path to remote storage.
   */
  def remotePath(path: String): UIO[String]

  /**
   * Upload file to remote storage.
   * @param targetPath allowed without schema or with pota-fs schema, for example: "aa/bb.txt", "pota://aa/bb.txt"
   */
  def upload(srcFile: File, targetPath: String): IO[FsErr, String]

  /**
   * Download file from remote storage to local target path.
   * @param srcPath allowed without schema or with pota-fs schema, for example: "aa/bb.txt", "pota://aa/bb.txt"
   */
  def download(srcPath: String, targetPath: String): IO[FsErr, File]

  /**
   * Download file as ZStream.
   * @param srcPath allowed without schema or with pota-fs schema.
   */
  def downloadAsStream(srcPath: String): Stream[FsErr, Byte]

  /**
   * Remove file from remote storage.
   */
  def remove(path: String): IO[FsErr, Unit]

  /**
   * Determine whether file exists on remote storage.
   */
  def exist(path: String): IO[FsErr, Boolean]

  final protected def purePath(path: String): UIO[String] = ZIO.succeed(paths.rmSchema andThen paths.rmFirstSlash apply path)
