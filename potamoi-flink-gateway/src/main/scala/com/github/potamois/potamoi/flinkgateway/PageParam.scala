package com.github.potamois.potamoi.flinkgateway

case class PageReq(index: Int, size: Int)

case class PageRsp(index: Int, size: Int, totalPages: Int, totalRow: Int, hasNext: Boolean)
