import potamoi.common.{ErrorExtension, FutureExtension, NumExtension, SttpExtension, Syntax, TimeExtension, ZIOExtension}

package object potamoi:

  val syntax  = Syntax
  val zios    = ZIOExtension
  val futures = FutureExtension
  val sttps   = SttpExtension
  val times   = TimeExtension
  val nums    = NumExtension
  val errs    = ErrorExtension

  def curTs: Long = System.currentTimeMillis
