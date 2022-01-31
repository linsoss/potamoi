package com.github.potamois.potamoi.flinkgateway

import org.apache.flink.table.api.{DataTypes, TableSchema}
import org.apache.flink.types.{Row, RowKind}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.language.postfixOps

class FlinkApiCovertToolSpec extends AnyWordSpec with Matchers {

  "FlinkApiCovertTool" should {

    "covertTableSchema" in {
      val schema = new TableSchema.Builder()
        .field("f1", DataTypes.STRING)
        .field("f2", DataTypes.INT)
        .field("f3", DataTypes.DATE())
        .field("f4", DataTypes.DOUBLE())
        .field("f5", DataTypes.BOOLEAN())
        .build()
      val cols = FlinkApiCovertTool.covertTableSchema(schema)
      cols shouldBe Seq(
        Column("f1", "STRING"),
        Column("f2", "INT"),
        Column("f3", "DATE"),
        Column("f4", "DOUBLE"),
        Column("f5", "BOOLEAN")
      )
    }

    "covertTableSchema with null schema" in {
      FlinkApiCovertTool.covertTableSchema(null) shouldBe Seq.empty
    }

    "covertRow" in {
      val row = Row.ofKind(RowKind.INSERT,
        "1",
        java.lang.Integer.valueOf(2),
        java.lang.Long.valueOf(3L),
        java.lang.Double.valueOf(4.0),
        java.lang.Boolean.valueOf(true),
        null)
      val rowData = FlinkApiCovertTool.covertRow(row)
      rowData.kind shouldBe "+I"
      rowData.values shouldBe Seq("1", "2", "3", "4.0", "true", "null")
    }

    "covertRow with null row" in {
      FlinkApiCovertTool.covertRow(null) shouldBe RowData.empty
    }
  }

}
