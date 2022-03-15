package com.github.potamois.potamoi.gateway.flink.interact

import com.github.potamois.potamoi.gateway.flink.{Error, PageRsp}

/**
 * Snapshot of Flink TableResult data for [[org.apache.flink.table.operations.QueryOperation]].
 *
 * @param data       table result data
 * @param error      error message that causes the failure of result collection.
 * @param isFinished whether the collection process is finished
 * @param lastTs     state last updated timestamp
 * @author Al-assad
 */
case class TableResultSnapshot(data: TableResultData, error: Option[Error], isFinished: Boolean, lastTs: Long)

/**
 * Snapshot of Flink TableResult data for [[org.apache.flink.table.operations.QueryOperation]]
 * from Pageable query.
 *
 * @param data       table result data
 * @param error      error message that causes the failure of result collection.
 * @param isFinished whether the collection process is finished
 * @param lastTs     state last updated timestamp
 * @author Al-assad
 */
case class PageableTableResultSnapshot(data: PageRsp[TableResultData], error: Option[Error], isFinished: Boolean, lastTs: Long)
