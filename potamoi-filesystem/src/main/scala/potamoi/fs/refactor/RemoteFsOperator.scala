package potamoi.fs.refactor

import potamoi.fs.refactor.FsErr
import potamoi.fs.refactor.backend.{LocalFsBackend, S3FsBackend, S3FsMirrorBackend}
import potamoi.syntax.contra
import potamoi.BaseConf
import zio.{IO, Scope, UIO, URIO, URLayer, ZIO, ZLayer}
import zio.stream.{Stream, ZStream}

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
  def actualPath(path: String): String

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
   * Download file from remote storage to local temporary directory.
   * @param srcPath allowed without schema or with pota-fs schema.
   */
  def download(srcPath: String): IO[FsErr, File]

  /**
   * Download file as ZStream.
   * @param srcPath allowed without schema or with pota-fs schema.
   */
  def downloadAsStream(srcPath: String): ZStream[Scope, FsErr, Byte]

  /**
   * Remove file from remote storage.
   */
  def remove(path: String): IO[FsErr, Unit]

  /**
   * Determine whether file exists on remote storage.
   */
  def exist(path: String): IO[FsErr, Boolean]

object RemoteFsOperator:

  val live: URLayer[FsBackendConf, RemoteFsOperator] = ZLayer {
    for {
      conf    <- ZIO.service[FsBackendConf]
      backend <- conf match
                   case c: LocalFsBackendConf                     => LocalFsBackend.make(c) <* LocalFsBackend.hintLog(c)
                   case c: S3FsBackendConf if c.enableMirrorCache => S3FsMirrorBackend.make(c) <* S3FsMirrorBackend.hintLog(c)
                   case c: S3FsBackendConf                        => S3FsBackend.make(c) <* S3FsBackend.hitLog(c)
    } yield backend
  }
