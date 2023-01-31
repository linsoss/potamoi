package potamoi.flink.model.interact

/**
 * The flink sql execution results presented as nano offsets.
 */
case class SqlResultOffsetView(
    lastOffset: Long,
    hasNextRow: Boolean,
    payload: PlainSqlRs)

object SqlResultOffsetView:

  def apply(plainRs: PlainSqlRs): SqlResultOffsetView = SqlResultOffsetView(
    lastOffset = plainRs.data.lastOption.map(_.nanoTs).getOrElse(-1),
    hasNextRow = false,
    payload = plainRs
  )
