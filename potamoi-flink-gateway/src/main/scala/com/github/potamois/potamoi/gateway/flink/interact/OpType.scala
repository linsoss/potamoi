package com.github.potamois.potamoi.gateway.flink.interact

/**
 * Flink sql operation type.
 *
 * 1) NORMAL: normal sql statement that executed in local, like "create ..., explain...";
 * 2) QUERY: query sql statement that executed in flink cluster, like "select ...";
 * 3) MODIFY: modify sql statement, like "insert ...";
 *
 * @author Al-assad
 */
object OpType extends Enumeration {
  type OpType = Value
  val UNKNOWN, NORMAL, QUERY, MODIFY = Value
}

