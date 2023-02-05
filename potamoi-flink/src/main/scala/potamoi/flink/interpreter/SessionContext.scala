package potamoi.flink.interpreter

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.delegation.Parser
import potamoi.flink.FlinkConfigurationTool.safeSet
import potamoi.flink.FlinkInterpreterErr.CreateTableEnvironmentErr
import potamoi.flink.model.interact.{RemoteClusterEndpoint, SessionSpec}
import potamoi.fs.refactor.RemoteFsOperator
import potamoi.syntax.{tap, toPrettyStr}
import zio.{IO, UIO, ZIO}
import zio.ZIO.{attempt, succeed}
import zio.direct.*
import zio.Console.printLine

import java.io.File
import java.net.URLClassLoader

case class SessionContext(
                           sessDef: SessionSpec,
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
  private[interpreter] def buildContext(
      sessionId: String,
      remoteFs: RemoteFsOperator,
      sessDef: SessionSpec): IO[CreateTableEnvironmentErr, SessionContext] = {
    defer {
      // download extra jars
      val (localJarFiles, clusterJarsFiles) = {
        val extraJars = (sessDef.localJars ++ sessDef.clusterJars).distinct
        if extraJars.isEmpty then succeed(List.empty -> List.empty).run
        else
          ZIO
            .foreachPar(extraJars)(jar => remoteFs.download(jar).map(jar -> _))
            .map { files =>
              files.filter { case (jar, _) => sessDef.localJars.contains(jar) }.map(_._2) ->
              files.filter { case (jar, _) => sessDef.clusterJars.contains(jar) }.map(_._2)
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
        conf.safeSet("pipeline.name", sessDef.jobName.getOrElse(SessionSpec.defaultJobName(sessionId)))
        conf.safeSet("pipeline.jars", clusterJarsFiles.map(file => s"file://${file.getAbsolutePath}"))

        sessDef.remoteEndpoint.foreach { case RemoteClusterEndpoint(address, port) =>
          conf.safeSet("rest.address", address)
          conf.safeSet("rest.port", port)
        }
        sessDef.extraProps
          .filterNot { case (k, _) => SessionSpec.nonAllowedOverviewConfigKeys.contains(k) }
          .foreach { case (k, v) => conf.safeSet(k, v) }
      }

      // resolve local classloader
      val classloader = attempt {
        val oriClassloader = getClass.getClassLoader
        if sessDef.localJars.isEmpty then oriClassloader
        else URLClassLoader.newInstance(localJarFiles.map(_.toURI.toURL).toArray, oriClassloader)
      }.run

      // create flink execution environment
      val env          = attempt(StreamExecutionEnvironment(configuration, classloader)).run
      val tEnvSettings = EnvironmentSettings.Builder().withConfiguration(configuration).withClassLoader(classloader).build
      val tEnv         = attempt(StreamTableEnvironment.create(env, tEnvSettings).asInstanceOf[TableEnvironmentInternal]).run
      val parser       = tEnv.getParser
      SessionContext(sessDef, configuration, classloader, env, tEnv, parser)
    }
  }.mapError(CreateTableEnvironmentErr(_))
