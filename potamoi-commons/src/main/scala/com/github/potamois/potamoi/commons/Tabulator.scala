package com.github.potamois.potamoi.commons

import org.apache.commons.text.StringEscapeUtils

import scala.collection.mutable
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
   * @param table      table content
   * @param escapeJava escapes the characters in a String using Java String rules.
   * @return Formatting tabulation string
   */
  def format(table: Seq[Seq[Any]], escapeJava: Boolean = true): String = table match {
    case Nil => ""
    case _ => Try {
      val cleanTable = covertCellToString(table, escapeJava)
      val cellSizes = cleanTable.map {
        _.map { c =>
          Option(c).fold(0) {
            case s: String => s.length
            case a => a.toString.length
          } + CellPadding
        }
      }
      val colSizes = cellSizes.transpose.map(_.max)
      val rows = cleanTable.map(r => formatRow(r, colSizes))
      formatRows(rowSeparator(colSizes), rows)
    }.getOrElse("")
  }

  private def covertCellToString(table: Seq[Seq[Any]], escapeJava: Boolean): mutable.Buffer[Seq[String]] =
    if (escapeJava) {
      table.map(_.map {
        case null => ""
        case s: String => StringEscapeUtils.escapeJava(s)
        case o => o.toString
      }).toBuffer
    } else {
      val unfoldTable = mutable.Buffer[Seq[String]]()
      // handle cell that contains multiple lines
      for (line <- table) {
        val foldCells = line.zipWithIndex.filter(e => e._1 != null && e._1.toString.contains(NL)).map(e => e._2 -> e._1.toString.split(NL))
        if (foldCells.isEmpty) {
          unfoldTable += line.map(cell => if (cell == null) "" else cell.toString)
        } else {
          val maxDepth = foldCells.map(_._2.length).max
          val foldCellsMap = foldCells.toMap
          val matrix = (0 until maxDepth).map(_ => Array.fill(line.length)("").toBuffer)
          // fill the first line of matrix
          for (y <- line.indices)
            if (!foldCellsMap.contains(y)) matrix.head(y) = if (line(y) == null) "" else line(y).toString
          // fill the unfold cells into the matrix
          for (y <- foldCellsMap.keys)
            for (x <- foldCellsMap(y).indices)
              matrix(x)(y) = foldCellsMap(y)(x)
          unfoldTable ++= matrix
        }
      }
      unfoldTable
    }

  private def formatRow(row: Seq[Any], colSizes: Seq[Int]): String = row.zip(colSizes).map {
    case (_, size: Int) if size == 0 => ""
    case (item, size) =>
      (" %-" + (size - CellPadding) + "s ").format(item)
  }.mkString("|", "|", "|")

  private def formatRows(separator: String, rows: Seq[String]) =
    (separator :: rows.head :: separator :: rows.tail.toList ::: separator :: Nil).mkString(NL)

  private def rowSeparator(colSizes: Seq[Int]) = colSizes.map(s => "-" * s).mkString("+", "+", "+")

}

