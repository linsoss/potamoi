package potamoi.flink.interp

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.Ignore
import potamoi.common.ZIOExtension.zioRun
import potamoi.flink.interp.model.SessionDef
import potamoi.flink.model.FlinkTargetType
import potamoi.flink.model.FlinkTargetType.Local
import potamoi.fs.refactor.RemoteFsOperator
import potamoi.fs.S3FsBackendConfDev
import potamoi.fs.refactor.backend.S3FsBackend
import potamoi.logger.PotaLogger
import potamoi.zios.*
import zio.{IO, Scope, Task, ZIO}

@Ignore
class SerialSqlExecutorTest extends AnyWordSpec:

  def testing[E, A](f: SerialSqlExecutor => ZIO[Scope, E, A]) = {
    (for {
      fs       <- ZIO.service[RemoteFsOperator]
      executor <- ZIO.succeed(SerialSqlExecutorImpl("23333", fs))
      rs       <- f(executor)
    } yield rs)
      .provide(S3FsBackendConfDev.asLayer >>> S3FsBackend.live, Scope.default)
      .provideLayer(PotaLogger.default)
      .run
  }

  "Execute sql normally" in testing { executor =>
    for {
      _ <- executor.initEnv(SessionDef(execType = Local))
      _ <- executor
             .executeSql("""CREATE TABLE Orders (
                           |    order_number BIGINT,
                           |    price        DECIMAL(32,2),
                           |    buyer        ROW<first_name STRING, last_name STRING>,
                           |    mset        MULTISET<STRING>,
                           |    order_time   TIMESTAMP(3)
                           |) WITH (
                           |  'connector' = 'datagen',
                           |  'number-of-rows' = '10'
                           |)
                           |""".stripMargin)
             .debugPretty
      _ <- executor.closeEnv
    } yield ()
  }

//  @main def testExecutor1 = testing { exec =>
//    exec.initEnvironment(SessionDef(FlinkTargetType.Local)) *>
//    exec.submitStatement("""CREATE TABLE Orders (
//                           |    order_number BIGINT,
//                           |    price        DECIMAL(32,2),
//                           |    buyer        ROW<first_name STRING, last_name STRING>,
//                           |    mset        MULTISET<STRING>,
//                           |    order_time   TIMESTAMP(3)
//                           |) WITH (
//                           |  'connector' = 'datagen',
//                           |  'number-of-rows' = '10'
//                           |)
//                           |""".stripMargin)
//
//  }

end SerialSqlExecutorTest
