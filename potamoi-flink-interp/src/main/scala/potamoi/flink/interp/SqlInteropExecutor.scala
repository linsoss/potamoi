package potamoi.flink.interp

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.delegation.Parser
import potamoi.flink.FlinkConf
import potamoi.syntax.tap
import potamoi.testing.Testing21.config
import potamoi.fs.refactor.RemoteFsOperator
import potamoi.testing.Testing233.config
import zio.IO

trait SqlInteropExecutor {

//  def initEnvironment(sessDef: InterpSessionDef): IO[FlinkInterpErr, Unit]
//  def execute(statement: String): Unit

}

private[interp] case class SessionContext(
    configuration: Configuration,
    classloader: ClassLoader,
    env: StreamExecutionEnvironment,
    tEnv: TableEnvironmentInternal,
    parser: Parser)

class SqlInteropExecutorLive(sessionId: String, sessDef: InterpSessionDef, flinkConf: FlinkConf, remoteFs: RemoteFsOperator) {

  // init
  def initContext(sessDef: InterpSessionDef) = {
    val configuration = Configuration().tap { conf =>
      conf.setString("execution.target", sessDef.execType.rawValue)
      conf.setString("execution.runtime-mode", sessDef.execMode.rawValue)
      conf.setInteger("parallelism.default", sessDef.parallelism)
      conf.setString("pipeline.name", sessDef.jobName.getOrElse(InterpSessionDef.defaultJobName(sessionId)))
      conf.setBoolean("execution.attached", true)
      conf.setBoolean("execution.shutdown-on-attached-exit", true)

      sessDef.remote.foreach { case RemoteClusterEndpoint(address, port) =>
        conf.setString("rest.address", address)
        conf.setInteger("rest.port", port)
      }
      sessDef.extraProps
        .filterNot { case (k, _) => InterpSessionDef.nonAllowedOverviewConfigKeys.contains(k) }
        .foreach { case (k, v) => conf.setString(k, v) }
    }
    val env = StreamExecutionEnvironment(configuration)
  }

}

@main def test = {}
