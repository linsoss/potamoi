package com.github.potamois.potamoi.gateway.flink

import com.github.potamois.potamoi.gateway.flink.interact.{Column, Row}
import com.github.potamois.potamoi.testkit.STSpec
import org.apache.flink.table.api.{DataTypes, TableSchema}
import org.apache.flink.types.{RowKind, Row => FlinkRow}

import scala.language.postfixOps

class FlinkApiCovertToolSpec extends STSpec {

  "FlinkApiCovertTool" should {

    "covertTableSchema correctly" in {
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

    "covertTableSchema with null schema correctly" in {
      FlinkApiCovertTool.covertTableSchema(null) shouldBe Seq.empty
    }

    "covertRow correctly" in {
      val row = FlinkRow.ofKind(RowKind.INSERT,
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

    "covertRow with null row correctly" in {
      FlinkApiCovertTool.covertRow(null) shouldBe Row.empty
    }
  }

}
