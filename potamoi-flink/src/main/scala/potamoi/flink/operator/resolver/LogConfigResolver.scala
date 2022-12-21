package potamoi.flink.operator.resolver

import potamoi.flink.ResolveClusterDefErr.ResolveLogConfigErr
import potamoi.fs.{lfs, LfsErr}
import potamoi.fs.LfsErr.IOErr
import zio.*
import zio.ZIO.{logInfo, succeed}

/**
 * Flink logging configuration resolver
 */
object LogConfigResolver {

  /**
   * Default flink log config including log4j and logback configs.
   */
  lazy val defaultFlinkLogConfigs = Map(
    "log4j-console.properties" -> defaultLog4jContent,
    "logback-console.xml"      -> defaultLogbackContent
  )

  lazy val flinkLogConfigNames = defaultFlinkLogConfigs.keys.toVector

  /**
   * Determine if the local files of flink logs configMap needed to deploy flink on k8s is ready.
   * write the default configuration file when the file does not exist.
   * See Flink config item: $internal.deployment.config-dir
   */
  def ensureFlinkLogsConfigFiles(confDir: String, overwrite: Boolean = false): IO[ResolveLogConfigErr, Unit] = {
    val isAllFilesExist = ZIO
      .foreachPar(flinkLogConfigNames.map(n => s"$confDir/$n"))(lfs.fileExists)
      .mapBoth(
        e => ResolveLogConfigErr(s"Unable to access flink log config directory: $confDir", e.cause),
        r => !r.contains(false)
      )
    val writeConfigFiles =
      ZIO.foreachDiscard(defaultFlinkLogConfigs) { case (fName, content) =>
        for {
          targetFilePath <- succeed(s"$confDir/$fName")
          _ <- lfs
            .write(targetFilePath, content)
            .mapError(e => ResolveLogConfigErr(s"Fail to write flink config file to local: $confDir/$fName", e.cause))
          _ <- logInfo(s"Wrote flink log config file: $targetFilePath")
        } yield ()
      }
    if (overwrite) writeConfigFiles.unit
    else writeConfigFiles.whenZIO(isAllFilesExist.map(r => !r)).unit
  }

  /**
   * Default log4j-console.properties content.
   */
  lazy val defaultLog4jContent: String =
    """# Allows this configuration to be modified at runtime. The file will be checked every 30 seconds.
      |monitorInterval=30
      |
      |# This affects logging for both user code and Flink
      |rootLogger.level = INFO
      |rootLogger.appenderRef.console.ref = ConsoleAppender
      |rootLogger.appenderRef.rolling.ref = RollingFileAppender
      |
      |# Uncomment this if you want to _only_ change Flink's logging
      |#logger.flink.name = org.apache.flink
      |#logger.flink.level = INFO
      |
      |# The following lines keep the log level of common libraries/connectors on
      |# log level INFO. The root logger does not override this. You have to manually
      |# change the log levels here.
      |logger.akka.name = akka
      |logger.akka.level = INFO
      |logger.kafka.name= org.apache.kafka
      |logger.kafka.level = INFO
      |logger.hadoop.name = org.apache.hadoop
      |logger.hadoop.level = INFO
      |logger.zookeeper.name = org.apache.zookeeper
      |logger.zookeeper.level = INFO
      |logger.shaded_zookeeper.name = org.apache.flink.shaded.zookeeper3
      |logger.shaded_zookeeper.level = INFO
      |
      |# Log all infos to the console
      |appender.console.name = ConsoleAppender
      |appender.console.type = CONSOLE
      |appender.console.layout.type = PatternLayout
      |appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n
      |
      |# Log all infos in the given rolling file
      |appender.rolling.name = RollingFileAppender
      |appender.rolling.type = RollingFile
      |appender.rolling.append = true
      |appender.rolling.fileName = ${sys:log.file}
      |appender.rolling.filePattern = ${sys:log.file}.%i
      |appender.rolling.layout.type = PatternLayout
      |appender.rolling.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n
      |appender.rolling.policies.type = Policies
      |appender.rolling.policies.size.type = SizeBasedTriggeringPolicy
      |appender.rolling.policies.size.size=100MB
      |appender.rolling.policies.startup.type = OnStartupTriggeringPolicy
      |appender.rolling.strategy.type = DefaultRolloverStrategy
      |appender.rolling.strategy.max = ${env:MAX_LOG_FILE_NUMBER:-10}
      |
      |# Suppress the irrelevant (wrong) warnings from the Netty channel handler
      |logger.netty.name = org.jboss.netty.channel.DefaultChannelPipeline
      |logger.netty.level = OFF
      |""".stripMargin

  /**
   * Default logback-console.properties content.
   */
  lazy val defaultLogbackContent: String =
    """<configuration>
      |    <appender name="console" class="ch.qos.logback.core.ConsoleAppender">
      |        <encoder>
      |            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{60} %X{sourceThread} - %msg%n</pattern>
      |        </encoder>
      |    </appender>
      |
      |    <appender name="rolling" class="ch.qos.logback.core.rolling.RollingFileAppender">
      |        <file>${log.file}</file>
      |        <append>false</append>
      |
      |        <rollingPolicy class="ch.qos.logback.core.rolling.FixedWindowRollingPolicy">
      |            <fileNamePattern>${log.file}.%i</fileNamePattern>
      |            <minIndex>1</minIndex>
      |            <maxIndex>10</maxIndex>
      |        </rollingPolicy>
      |
      |        <triggeringPolicy class="ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy">
      |            <maxFileSize>100MB</maxFileSize>
      |        </triggeringPolicy>
      |
      |        <encoder>
      |            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{60} %X{sourceThread} - %msg%n</pattern>
      |        </encoder>
      |    </appender>
      |
      |    <!-- This affects logging for both user code and Flink -->
      |    <root level="INFO">
      |        <appender-ref ref="console"/>
      |        <appender-ref ref="rolling"/>
      |    </root>
      |
      |    <!-- Uncomment this if you want to only change Flink's logging -->
      |    <!--<logger name="org.apache.flink" level="INFO"/>-->
      |
      |    <logger name="akka" level="INFO"/>
      |    <logger name="org.apache.kafka" level="INFO"/>
      |    <logger name="org.apache.hadoop" level="INFO"/>
      |    <logger name="org.apache.zookeeper" level="INFO"/>
      |
      |    <!-- Suppress the irrelevant (wrong) warnings from the Netty channel handler -->
      |    <logger name="org.jboss.netty.channel.DefaultChannelPipeline" level="ERROR"/>
      |</configuration>
      |""".stripMargin
}
