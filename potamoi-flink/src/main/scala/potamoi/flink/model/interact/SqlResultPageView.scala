package potamoi.flink.model.interact

import potamoi.KryoSerializable

/**
 * Results of flink sql execution rendered in pagination.
 */
case class SqlResultPageView(
    totalPage: Int,
    pageIndex: Int,
    hasNextPage: Boolean,
    hasNextRowThisPage: Boolean,
    payload: PlainSqlRs)
    extends KryoSerializable

object SqlResultPageView:

  def apply(plainRs: PlainSqlRs): SqlResultPageView = SqlResultPageView(
    totalPage = 1,
    pageIndex = 1,
    hasNextPage = false,
    hasNextRowThisPage = false,
    payload = plainRs
  )
