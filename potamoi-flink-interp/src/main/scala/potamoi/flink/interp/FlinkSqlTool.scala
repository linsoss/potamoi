package potamoi.flink.interp

import scala.collection.mutable.ArrayBuffer

object FlinkSqlTool:

  /**
   *  Split flink sql script into multiple sql statements.
   */
  def splitSqlScript(scripts: String): Array[String] = {
    scripts
      .concat("\n")
      .split(";\\s*\n")
      .map(_.trim)
      .filter(!_.isBlank)
      .foldLeft[(ArrayBuffer[String], Option[String])](ArrayBuffer.empty -> None) { case ((result, buff), sql) =>
        sql.toUpperCase match
          case s if s.startsWith("EXECUTE STATEMENT SET") => result                                                   -> Some(sql)
          case "END"                                      => (result :+ buff.getOrElse("").concat(";\n").concat(sql)) -> None
          case _ =>
            buff match
              case Some(b) => result          -> Some(b.concat(";\n").concat(sql))
              case None    => (result :+ sql) -> buff
      }
      ._1
      .toArray
  }




