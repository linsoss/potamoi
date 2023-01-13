import potamoi.common.*

package object potamoi:

  val syntax   = Syntax
  val zios     = ZIOExtension
  val futures  = FutureExtension
  val sttps    = SttpExtension
  val times    = TimeExtension
  val nums     = NumExtension
  val codecs   = Codec
  val uuids    = Uuid
  val collects = CollectionExtension

  def curTs: Long = System.currentTimeMillis
