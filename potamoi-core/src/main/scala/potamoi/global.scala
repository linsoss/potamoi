package potamoi

import potamoi.common.{FutureExtension, NumExtension, SttpExtension, Syntax, TimeExtension, ZIOExtension}

val syntax  = Syntax
val ziox    = ZIOExtension
val futurex = FutureExtension
val sttpx   = SttpExtension
val timex   = TimeExtension
val numx    = NumExtension

def curTs: Long = System.currentTimeMillis
