package potamoi.fs.refactor

import org.apache.tika.Tika
import potamoi.common.Syntax.contra

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
   * Remove the slash at the beginning of the path.
   */
  def rmFirstSlash(path: String): String = if path.startsWith("/") then path.substring(1, path.length) else path

  /**
   * Get file name from path.
   */
  def getFileName(path: String): String = path.split('/').last

  /**
   * Detect mime type of file.
   */
  def detectMimeType(path: String): String = Try(Tika().detect(path)).getOrElse("")
  def detectMimeType(file: File): String   = Try(Tika().detect(file)).getOrElse("")
