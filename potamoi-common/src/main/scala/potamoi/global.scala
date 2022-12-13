package potamoi

import potamoi.common.{FutureExtension, NumExtension, SttpExtension, Syntax, TimeExtension, ZIOExtension}

val syntax  = Syntax
val zios    = ZIOExtension
val futures = FutureExtension
val sttps   = SttpExtension
val times   = TimeExtension
val nums    = NumExtension

def curTs: Long = System.currentTimeMillis
