package potamoi.fs

import org.apache.tika.Tika
import potamoi.common.Syntax.contra
import zio.{UIO, ZIO}

import java.io.File
import scala.util.Try

/**
 * Tools for handling file paths.
 */
val paths = PathTool

object PathTool:

  /**
   * Potamoi remote fs schema.
   */
  val potaFsSchema: String                 = "pota"
  def withPotaSchema(path: String): String = s"$potaFsSchema://$path"

  /**
   * S3 storage prefix
   */
  val s3SchemaPrefix = Vector("s3", "s3a", "s3n", "s3p")

  /**
   * Extract schema from path.
   */
  def getSchema(path: String): Option[String] = path.split("://").contra { segs =>
    if segs.length < 2 then None else segs.headOption
  }

  /**
   * Remove path schema likes from "s3://bucket/xx.jar" to "bucket/xx.jar".
   */
  def rmSchema(path: String): String = path.split("://").last

  /**
   * Determine if the file path is pota schema
   */
  def isPotaPath(path: String): Boolean = getSchema(path).contains(potaFsSchema)

  /**
   * Remove the slash at the beginning of the path.
   */
  def rmFirstSlash(path: String): String = if path.startsWith("/") then path.substring(1, path.length) else path

  /**
   * Remove path schema and first slash.
   */
  def purePath(path: String) = rmSchema andThen rmFirstSlash apply path

  /**
   * Get file name from path.
   */
  def getFileName(path: String): String = path.split('/').last

  /**
   * Detect mime type of file.
   */
  def detectMimeType(path: String): String = Try(Tika().detect(path)).getOrElse("")
  def detectMimeType(file: File): String   = Try(Tika().detect(file)).getOrElse("")

  /**
   * Modify s3 path to s3p schema.
   */
  def convertS3ToS3pSchema(path: String): String = path.split("://").contra { segs =>
    if segs.length < 2 then path
    else if !s3SchemaPrefix.contains(segs(0)) then path
    else s"s3p://${segs(1)}"
  }

  /**
   * Determine if the file path is s3 schema.
   */
  def isS3Path(path: String): Boolean = s3SchemaPrefix.contains(path.split("://").head.trim)
