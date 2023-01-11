package potamoi.flink.interp

import potamoi.common.ZIOExtension.zioRun
import potamoi.flink.interp.model.SessionDef
import potamoi.flink.model.FlinkTargetType
import potamoi.fs.refactor.RemoteFsOperator
import potamoi.fs.S3FsBackendConfDev
import potamoi.fs.refactor.backend.S3FsBackend
import potamoi.zios.*
import zio.{IO, Task, ZIO}

object SqlExecutorTest:

  def testing[E, A](f: SqlExecutor => IO[E, A]) = {
    (for {
      fs       <- ZIO.service[RemoteFsOperator]
      executor <- SqlExecutor.instance("23333", fs)
      rs       <- f(executor)
    } yield rs)
      .provide(S3FsBackendConfDev.asLayer >>> S3FsBackend.live)
      .run
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
