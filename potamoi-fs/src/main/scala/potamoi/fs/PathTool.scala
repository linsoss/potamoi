package potamoi.fs

import potamoi.common.Syntax.contra

/**
 * Tools for handling file paths.
 */
val paths = PathTool

object PathTool:

  /**
   * remove path schema likes from "s3://bucket/xx.jar" to "bucket/xx.jar".
   */
  def purePath(path: String): String = path.split("://").last.contra(p => if (p.startsWith("/")) p.substring(1, p.length) else p)

  /**
   * Remove the stash at the beginning of the path.
   */
  def rmSlashPrefix(path: String): String = if (path.startsWith("/")) path.substring(1, path.length) else path

  /**
   * Get file name from path.
   */
  def getFileName(path: String): String = path.split('/').last

  /**
   * S3 storage prefix
   */
  val s3SchemaPrefix = Vector("s3", "s3a", "s3n", "s3p")

  /**
   * Determine if the file path is s3 schema.
   */
  def isS3Path(path: String): Boolean = s3SchemaPrefix.contains(path.split("://").head.trim)

  /**
   * Modify s3 path to s3p schema.
   */
  def reviseToS3pSchema(path: String): String = {
    path.split("://").contra { segs =>
      if (segs.length < 2) path
      else if (!s3SchemaPrefix.contains(segs(0))) path
      else s"s3p://${segs(1)}"
    }
  }
