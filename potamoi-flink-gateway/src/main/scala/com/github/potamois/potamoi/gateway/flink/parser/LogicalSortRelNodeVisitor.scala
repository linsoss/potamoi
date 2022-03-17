package com.github.potamois.potamoi.gateway.flink.parser

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.logical.LogicalSort

import scala.collection.mutable

/**
 * CalciteRelShuttle for visiting the [[LogicalSort]]
 *
 * @author Al-assad
 */
class LogicalSortRelNodeVisitor extends CalciteRelShuttle {

  private val sortNodes = mutable.Buffer.empty[LogicalSort]

  override def visit(logicalSort: LogicalSort): RelNode = {
    sortNodes += logicalSort
    super.visit(logicalSort)
  }

  /**
   * Get all [[LogicalSort]] RexNodes that find.
   */
  def getSortNodes: Seq[LogicalSort] = sortNodes.toSeq

}
