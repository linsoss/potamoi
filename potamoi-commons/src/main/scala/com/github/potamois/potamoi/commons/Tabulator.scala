package com.github.potamois.potamoi.commons

import scala.util.Try

/**
 * Ascii table formatter.
 *
 * @author Al-assad
 */
object Tabulator {

  private val CellPadding = 2
  private val NL = "\n"

  /**
   * Generate an ASCII table for a given data set
   *
   * @param table table content
   * @return Formatting tabulation string
   */
  def format(table: Seq[Seq[Any]]): String = Try(table match {
    case Nil => ""
    case _ =>
      val cellSizes = table.map {
        _.map { c =>
          Option(c).fold(0) {
            case s: String => s.length
            case a => a.toString.length
          } + CellPadding
        }
      }
      val colSizes = cellSizes.transpose.map(_.max)
      val rows = table.map(r => formatRow(r, colSizes))
      formatRows(rowSeparator(colSizes), rows)
  }).getOrElse("")

  private def formatRow(row: Seq[Any], colSizes: Seq[Int]) = row.zip(colSizes)
    .map {
      case (_, size: Int) if size == 0 => ""
      case (item, size) =>
        (" %-" + (size - CellPadding) + "s ").format(item)
    }.mkString("|", "|", "|")

  private def formatRows(separator: String, rows: Seq[String]) =
    (separator :: rows.head :: separator :: rows.tail.toList ::: separator :: Nil).mkString(NL)

  private def rowSeparator(colSizes: Seq[Int]) = colSizes.map(s => "-" * s).mkString("+", "+", "+")

}

