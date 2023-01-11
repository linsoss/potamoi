package potamoi.flink.interp

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.delegation.Parser
import potamoi.flink.interp.model.{RemoteClusterEndpoint, SessionDef}
import potamoi.flink.FlinkConfigurationTool.safeSet
import potamoi.fs.refactor.RemoteFsOperator
import potamoi.syntax.tap
import zio.{IO, UIO, ZIO}
import zio.ZIO.{attempt, succeed}
import zio.direct.*

import java.net.URLClassLoader

case class SessionContext(
    sessDef: SessionDef,
    configuration: Configuration,
    classloader: ClassLoader,
    env: StreamExecutionEnvironment,
    tEnv: TableEnvironmentInternal,
    parser: Parser):
  def close: UIO[Unit] = ZIO.attempt(env.close()).ignore

object SessionContext:

  /**
   * Create and initialize SessionContext.
   */
  def buildContext(sessionId: String, sessDef: SessionDef, remoteFs: RemoteFsOperator): IO[Throwable, SessionContext] = defer {
    // download extra jars
    val (jarsFiles, sentClusterJarsFiles) = {
      val extraJars = (sessDef.jars ++ sessDef.sentClusterJars).distinct
      if extraJars.isEmpty then succeed(List.empty -> List.empty).run
      else
        ZIO
          .foreachPar(extraJars)(jar => remoteFs.download(jar).map(jar -> _))
          .map { files =>
            files.filter { case (jar, _) => sessDef.jars.contains(jar) }.map(_._2) ->
            files.filter { case (jar, _) => sessDef.sentClusterJars.contains(jar) }.map(_._2)
          }
          .run
    }
    // resolve flink configuration
    val configuration = Configuration().tap { conf =>
      conf.safeSet("execution.target", sessDef.execType.rawValue)
      conf.safeSet("execution.runtime-mode", sessDef.execMode.rawValue)
      conf.safeSet("execution.attached", true)
      conf.safeSet("execution.shutdown-on-attached-exit", true)

      conf.safeSet("parallelism.default", sessDef.parallelism)
      conf.safeSet("pipeline.name", sessDef.jobName.getOrElse(SessionDef.defaultJobName(sessionId)))
      conf.safeSet("pipeline.jars", sentClusterJarsFiles.map(_.getAbsolutePath))

      sessDef.remote.foreach { case RemoteClusterEndpoint(address, port) =>
        conf.safeSet("rest.address", address)
        conf.safeSet("rest.port", port)
      }
      sessDef.extraProps
        .filterNot { case (k, _) => SessionDef.nonAllowedOverviewConfigKeys.contains(k) }
        .foreach { case (k, v) => conf.safeSet(k, v) }
    }

    // resolve local classloader
    val classloader = attempt {
      val oriClassloader = getClass.getClassLoader
      if sessDef.jars.isEmpty then oriClassloader
      else URLClassLoader.newInstance(jarsFiles.map(_.toURI.toURL).toArray, oriClassloader)
    }.run

    // create flink execution environment
    val env    = attempt(StreamExecutionEnvironment(configuration, classloader)).run
    val tEnv   = attempt(StreamTableEnvironment.create(env).asInstanceOf[TableEnvironmentInternal]).run
    val parser = tEnv.getParser
    SessionContext(
      sessDef = sessDef,
      configuration = configuration,
      classloader = classloader,
      env = env,
      tEnv = tEnv,
      parser = parser
    )
  }
