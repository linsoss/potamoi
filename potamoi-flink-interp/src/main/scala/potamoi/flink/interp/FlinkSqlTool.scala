package potamoi.flink.interp

import org.apache.calcite.config.Lex
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.pretty.SqlPrettyWriter
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl
import zio.{Task, ZIO}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*

object FlinkSqlTool:

  /**
   * Create flink sql calcite parser.
   */
  private[flink] def createParser(expr: String): SqlParser = {
    val parserConfig = SqlParser.config
      .withParserFactory(FlinkSqlParserImpl.FACTORY)
      .withLex(Lex.JAVA)
      .withIdentifierMaxLength(256)
    SqlParser.create(expr, parserConfig)
  }

  /**
   * Split flink sql script into multiple sql statements.
   */
  def splitSqlScript(sqlScript: String): Task[List[String]] = ZIO.attempt {
    val parser = createParser(sqlScript)
    val stmts  = parser.parseStmtList().asScala.map(_.toString).toList
    stmts
  }
