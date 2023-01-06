import potamoi.common.*

package object potamoi:

  val syntax  = Syntax
  val zios    = ZIOExtension
  val futures = FutureExtension
  val sttps   = SttpExtension
  val times   = TimeExtension
  val nums    = NumExtension
  val errs    = ErrorExtension
  val codecs  = Codec

  def curTs: Long = System.currentTimeMillis
