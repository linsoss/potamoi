package potamoi.common

object Uuid:

  /**
   * Generate a uuid of length 36
   */
  def genUUID36: String = java.util.UUID.randomUUID().toString

  /**
   * Generate a uuid of length 32
   */
  def genUUID32: String = genUUID36.split("-").mkString

  /**
   * Generate a uuid of length 16
   */
  def genUUID16: String = genUUID32.substring(0, 16)

  /**
   * Generate a uuid of custom length which should be less than 32
   */
  def genUUID(size: Int): String = if (size >= 32) genUUID32 else genUUID32.substring(0, size)
