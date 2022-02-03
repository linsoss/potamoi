package com.github.potamois.potamoi.flinkgateway

import scala.util.matching.Regex

/**
 * Flink sql parser tools.
 *
 * @author Al-assad
 */
object FlinkSqlParser {

  private val SINGLE_CMT_PATTERN_1: Regex = "//.*".r
  private val SINGLE_CMT_PATTERN_2: Regex = "--.*".r
  private val MULTI_CMT_PATTERN: Regex = "/\\*[\\s\\S]*?\\*/".r

  /**
   * Extract the valid sql sequence from the given sql string, using ";" as a separator
   * and removing the comment content from it.
   *
   * Supported symbols:
   * 1) sql separator: ";"
   * 2）single line comment: "//" or "--"
   * 3) multi line comments: "/* */"
   *
   * @param sql Flink sqls separated by ";"
   * @return Effective sql sequence.
   *         It would return empty Seq when it sql contains unclosed "/*" or "*/"
   */
  def splitSqlStatement(sql: String): Seq[String] = sql match {
    case null => Seq.empty
    case _ => var rsql = sql
      // remove multiple lines comment like "/* ... */"
      if (rsql.contains("/*")) rsql = MULTI_CMT_PATTERN.replaceAllIn(rsql, "")
      if (rsql.contains("/*") || rsql.contains("*/")) return Seq.empty
      // remove single line comment like "// ..." and "-- ..."
      if (rsql.contains("//")) rsql = SINGLE_CMT_PATTERN_1.replaceAllIn(rsql, "")
      if (rsql.contains("--")) rsql = SINGLE_CMT_PATTERN_2.replaceAllIn(rsql, "")
      // remove the blank line and trailing blank, then split with ";"
      rsql.split("\n").filter(!_.isBlank).map(_.stripTrailing).mkString("\n")
        .split(";").map(_.trim).filter(_.nonEmpty).toList
  }


}
