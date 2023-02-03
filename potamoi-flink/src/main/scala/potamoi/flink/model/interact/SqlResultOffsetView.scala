package potamoi.flink.model.interact

import potamoi.KryoSerializable

/**
 * The flink sql execution results presented as nano offsets.
 */
case class SqlResultOffsetView(
    lastOffset: Long,
    hasNextRow: Boolean,
    payload: PlainSqlRs)
    extends KryoSerializable

object SqlResultOffsetView:

  def apply(plainRs: PlainSqlRs): SqlResultOffsetView = SqlResultOffsetView(
    lastOffset = plainRs.data.lastOption.map(_.nanoTs).getOrElse(-1),
    hasNextRow = false,
    payload = plainRs
  )
