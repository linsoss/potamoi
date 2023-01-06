package potamoi.flink.interp

import org.apache.flink.configuration.Configuration
import potamoi.syntax.contra
import potamoi.Testing21.config

trait SqlInteropExecutor {

  def execute(sql: String): Unit

}

class SqlInteropExecutorLive(sessionId: String, sessDef: SessionDef) {

  val configuration = Configuration().contra { conf =>
    sessDef.mode
    conf.setString("execution.target", "local")
    sessDef.jobName.foreach(conf.setString("pipeline.name", _))

    conf.setBoolean("execution.attached", true)
    conf.setBoolean("execution.shutdown-on-attached-exit", true)
  }

}

@main def test = {}
