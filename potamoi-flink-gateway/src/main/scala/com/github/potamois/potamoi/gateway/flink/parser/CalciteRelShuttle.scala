package com.github.potamois.potamoi.gateway.flink.parser

import org.apache.calcite.rel.core.{TableFunctionScan, TableScan}
import org.apache.calcite.rel.logical._
import org.apache.calcite.rel.{RelNode, RelShuttle}

/**
 * Silent implementation of [[org.apache.calcite.rel.RelShuttle]].
 *
 * @author Al-assad
 */
trait CalciteRelShuttle extends RelShuttle {

  override def visit(tableScan: TableScan): RelNode = tableScan

  override def visit(tableFunctionScan: TableFunctionScan): RelNode = tableFunctionScan

  override def visit(logicalValues: LogicalValues): RelNode = logicalValues

  override def visit(logicalFilter: LogicalFilter): RelNode = logicalFilter

  override def visit(logicalCalc: LogicalCalc): RelNode = logicalCalc

  override def visit(logicalProject: LogicalProject): RelNode = logicalProject

  override def visit(logicalJoin: LogicalJoin): RelNode = logicalJoin

  override def visit(logicalCorrelate: LogicalCorrelate): RelNode = logicalCorrelate

  override def visit(logicalUnion: LogicalUnion): RelNode = logicalUnion

  override def visit(logicalIntersect: LogicalIntersect): RelNode = logicalIntersect

  override def visit(logicalMinus: LogicalMinus): RelNode = logicalMinus

  override def visit(logicalAggregate: LogicalAggregate): RelNode = logicalAggregate

  override def visit(logicalMatch: LogicalMatch): RelNode = logicalMatch

  override def visit(logicalSort: LogicalSort): RelNode = logicalSort

  override def visit(logicalExchange: LogicalExchange): RelNode = logicalExchange

  override def visit(logicalTableModify: LogicalTableModify): RelNode = logicalTableModify

  override def visit(relNode: RelNode): RelNode = relNode
}
