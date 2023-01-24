package potamoi.flink.interpreter

import org.scalatest.wordspec.AnyWordSpec
import potamoi.zios.runUnsafe
import potamoi.zios.debugPretty
import potamoi.syntax.contra
import zio.Console.printLine

class FlinkSqlToolSpec extends AnyWordSpec:

  "split sql script case1" in {
    val scripts: String =
      """
        |CREATE TABLE Orders (
        |    order_number BIGINT,
        |    price        DECIMAL(32,2),
        |    buyer        ROW<first_name STRING, last_name STRING>,
        |    mset        MULTISET<STRING>,
        |    order_time   TIMESTAMP(3)
        |) WITH (
        |  'connector' = 'datagen',
        |  'number-of-rows' = '20'
        |);
        |
        |
        |SHOW CATALOGS;
        |SHOW TABLES;
        |SHOW MODULES;
        |
        |SET 'sql-client.execution.result-mode' = 'tableau';
        |SET 'execution.runtime-mode' = 'batch';
        |SET;
        |
        |SELECT * from Orders;
        |SELECT
        |  name,
        |  COUNT(*) AS cnt
        |FROM
        |  (VALUES ('Bob'), ('Alice'), ('Greg'), ('Bob')) AS NameTable(name)
        |GROUP BY name;
        |
        |ADD JAR '/path/hello.jar';
        |REMOVE JAR '/path/hello.jar';
        |
        |LOAD MODULE hive WITH ('hive-version' = '3.1.3');
        |
        |ANALYZE TABLE Store COMPUTE STATISTICS FOR ALL COLUMNS;
        |EXPLAIN select * FROM Orders;
        |
        |; ;
        |
        |INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%';
        |
        |EXECUTE STATEMENT SET
        |BEGIN
        |INSERT INTO students
        |  VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);
        |INSERT INTO students
        |  VALUES ('fred flintstone', 35, 1.28), ('barney rubble', 32, 2.32);
        |END;
        |""".stripMargin

    FlinkSqlTool.splitSqlScript(scripts).debugPretty.runUnsafe.contra(r => assert(r.size == 16))
  }

  "split sql script case2" in {
    val scripts =
      """// comment1
        |/* comment2 */
        |CREATE TABLE Orders (
        |    order_number BIGINT,
        |    price        DECIMAL(32,2),
        |    buyer        ROW<first_name STRING, last_name STRING>,
        |    mset        MULTISET<STRING>,
        |    order_time   TIMESTAMP(3)
        |) WITH (
        |  'connector' = 'datagen',
        |  'number-of-rows' = '20'
        |);
        |/**
        |* select * from Orders
        |*/
        | SHOW TABLES;
        |""".stripMargin

    FlinkSqlTool.splitSqlScript(scripts).debugPretty.runUnsafe.contra(r => assert(r.size == 2))
  }
