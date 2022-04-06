package com.github.potamois.potamoi.gateway.flink.parser

import org.apache.flink.table.planner.operations.PlannerQueryOperation

import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
 * Flink sql parser tools.
 *
 * @author Al-assad
 */
object FlinkSqlParser {

  /**
   * Extract the valid sql sequence from the given sql string, using ";" as delimiter
   * and removing the comment content from it.
   *
   * Supported symbols:
   * 1) sql delimiter: ";"
   * 2）single-line comment: "--"
   * 3) multi-line comments: "/* */"
   *
   * @param sql Flink sqls separated by ";"
   * @return Effective sql sequence.
   */
  def extractSqlStatements(sql: String): Seq[String] = sql match {
    case null => List.empty
    case _ =>
      val preProcSqlChars = removeSqlMultiLineComments(sql)
      val collect = removeSingleCommentAndSplitSql(preProcSqlChars)
      // remove the blank line, then trim each line
      collect.filter(!_.isBlank)
        .map(stmt => stmt.split("\n").filter(!_.isBlank).map(_.stripTrailing).mkString("\n").trim)
        .toList
  }

  private case class DualToken(var on: Boolean = false, var idx: Int = -2) {
    def reset: DualToken = {
      on = false
      idx = -2
      this
    }
  }

  /**
   * Remove multiple line comment comments /* ... */ from sql.
   * Note that additional handling is required when the comment symbols is
   * included in a single quote.
   */
  private def removeSqlMultiLineComments(sql: String): Seq[Char] = {

    var buf = ListBuffer.empty[Char]
    // single quote token ''
    var sglQuoteToken = false
    // comment token /*
    val cmtBeginToken = ListBuffer(DualToken())
    // comment token */
    val cmtEndToken = DualToken()

    type isLast = Boolean

    def bufIdx = buf.length

    def existCmtBeginToken: (Boolean, isLast) =
      if (cmtBeginToken.last.on) true -> true
      else if (cmtBeginToken.size > 1 && cmtBeginToken(cmtBeginToken.length - 2).on) true -> false
      else false -> true

    def rmLastCmtBeginToken =
      if (cmtBeginToken.size > 1) cmtBeginToken.remove(cmtBeginToken.length - 1)
      else cmtBeginToken.last.reset

    for (ch <- sql) {
      ch match {
        case '\'' =>
          sglQuoteToken = !sglQuoteToken
          buf += ch
        case '*' =>
          if (cmtBeginToken.last.on) cmtEndToken.idx = bufIdx
          else {
            if (cmtBeginToken.last.idx + 1 == bufIdx) cmtBeginToken.last.on = true
            else cmtEndToken.idx = bufIdx
          }
          // log.debug(s"* => bufIdx: $bufIdx, mulCmtBegin: $cmtBeginToken, mulCmtEnd: $cmtEndToken")
          buf += ch
        case '/' =>
          if (cmtEndToken.idx + 1 == bufIdx) {
            existCmtBeginToken match {
              case (false, _) => buf += ch
              case (true, true) =>
                if (sglQuoteToken) buf += ch
                else {
                  buf = buf.take(cmtBeginToken.last.idx)
                  rmLastCmtBeginToken
                }
              case (true, false) =>
                if (sglQuoteToken) buf += ch
                else {
                  buf = buf.take(cmtBeginToken(cmtBeginToken.length - 2).idx)
                  rmLastCmtBeginToken
                  cmtBeginToken.last.reset
                }
            }
            // log.debug(s"/ end => bufIdx: ${_bufIdx}, mulCmtBegin: $cmtBeginToken, mulCmtEnd: $cmtEndToken")
            cmtEndToken.reset
          } else {
            existCmtBeginToken match {
              case (true, true) => cmtBeginToken += DualToken(idx = bufIdx)
              case (true, false) => cmtBeginToken.last.idx = bufIdx
              case (false, _) => cmtBeginToken.last.idx = bufIdx
            }
            // log.debug(s"/ => bufIdx: $bufIdx, mulCmtBegin: $cmtBeginToken, mulCmtEnd: $cmtEndToken")
            buf += ch
          }
        case _ =>
          buf += ch
      }
    }
    buf
  }

  /**
   * Remove single line comment like "--" and split sql with ";".
   * Note the handling of semicolons contained in single quotes,
   * such as "select * from ta where f1 like '%;%' ".
   */
  private def removeSingleCommentAndSplitSql(sqlChars: Seq[Char]): Seq[String] = {

    val collect = ListBuffer.empty[String]
    var buf = ListBuffer.empty[Char]
    // single quote token ''
    var sglQuoteToken = false
    // single-line comment symbol --
    val cmtToken = DualToken()

    def bufIdx = buf.length

    for (ch <- sqlChars) {
      ch match {
        case ';' =>
          if (sglQuoteToken || cmtToken.on) buf += ch
          else {
            if (cmtToken.on) buf = buf.take(cmtToken.idx)
            collect += buf.mkString
            buf.clear
            cmtToken.reset
          }
        case '\'' =>
          sglQuoteToken = !sglQuoteToken
          buf += ch
        case '-' =>
          cmtToken match {
            case DualToken(_, idx) if idx < 0 => cmtToken.idx = bufIdx
            case DualToken(_, idx) if idx + 1 == bufIdx => cmtToken.on = true
            case DualToken(false, _) => cmtToken.reset
            case _ =>
          }
          buf += ch
        case '\n' =>
          if (cmtToken.on) {
            buf = buf.take(cmtToken.idx)
            cmtToken.reset
          }
          buf += ch
        case _ =>
          buf += ch
      }
    }
    if (buf.nonEmpty) {
      if (cmtToken.on) buf = buf.take(cmtToken.idx)
      collect += buf.mkString
    }
    collect
  }


  /**
   * Get the value of topmost fetch RexNode if exists from Flink [[PlannerQueryOperation]].
   */
  def getTopmostLimitRexFromOp(operation: PlannerQueryOperation): Option[Int] = {
    val visitor = new LogicalSortRelNodeVisitor
    operation.getCalciteTree.accept(visitor)
    val sortNodes = visitor.getSortNodes
    Try(sortNodes.headOption.map(node => node.fetch.toString.toInt)).getOrElse(None)
  }


}
