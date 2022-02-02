package com.github.potamois.potamoi.flinkgateway

import scala.util.matching.Regex

/**
 * @author Al-assad
 */
object FlinkSqlParser {

  private val SINGLE_CMT_PATTERN_1: Regex = "//.*".r
  private val SINGLE_CMT_PATTERN_2: Regex = "--.*".r

  def splitSqlStatement(sql: String): Seq[String] = sql match {
    case null => Seq.empty
    case _ => var rsql = sql
      // remove single line comment like "// ..." and "-- ..."
      if (rsql.contains("//")) rsql = SINGLE_CMT_PATTERN_1.replaceAllIn(rsql, "")
      if (rsql.contains("--")) rsql = SINGLE_CMT_PATTERN_2.replaceAllIn(rsql, "")
      // remove multiple lines comment like "/* ... */"
      // todo
      // split lines with ";"
      rsql.split(";").map(_.trim).filter(_.nonEmpty).toList
  }


}
