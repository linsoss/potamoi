package potamoi.flink.operator.resolver

import cats.Eval
import com.softwaremill.quicklens.modify
import org.apache.flink.configuration.Configuration
import potamoi.collects.filterNotBlank
import potamoi.flink.model.deploy.*
import potamoi.flink.FlinkConf
import potamoi.flink.FlinkConfigurationTool.{mergeMap, mergeProp, mergePropOpt, safeSet}
import potamoi.flink.ResolveFlinkClusterSpecErr.{ConvertToFlinkRawConfigErr, ReviseClusterSpecErr}
import potamoi.flink.operator.resolver.ClusterSpecResolver.*
import potamoi.fs.{paths, FsBackendConf, S3AccessStyle, S3FsBackendConf}
import potamoi.syntax.{contra, safeTrim}
import potamoi.zios.asLayer
import zio.{IO, ZIO}

/**
 * Flink cluster definition resolver for [[ClusterSpec]].
 */
object ClusterSpecResolver {

  val session = SessionClusterSpecResolver
  val jarApp  = JarAppClusterSpecResolver
//  val sqlApp = SqlAppClusterSpecResolver

  /**
   * Flink raw configuration keys that are not allowed to be customized by users.
   */
  lazy val NotAllowCustomFlinkConfigKeys: Vector[String] = Vector(
    "execution.target",
    "kubernetes.cluster-id",
    "kubernetes.namespace",
    "kubernetes.container.image",
    "kubernetes.service-account",
    "kubernetes.jobmanager.service-account",
    "kubernetes.pod-template-file",
    "kubernetes.pod-template-file.taskmanager",
    "kubernetes.pod-template-file.jobmanager",
    "$internal.deployment.config-dir",
    "pipeline.jars",
    "$internal.application.main",
    "$internal.application.program-args")

  /**
   * The keys in the flink configuration that may need to be protected.
   */
  lazy val ProtectedFlinkConfigKeys: Vector[String] = Vector(
    "hive.s3.aws-secret-key",
    "fs.s3a.secret.key",
    "s3.secret-key"
  )

  /**
   * Injected config to to mark this flink cluster is deployed by potamoi.
   */
  lazy val InjectedDeploySourceConf = "deploy.source" -> "potamoi"

  /**
   * Injected config to to mark the actual flink execution.target.
   * value: [[potamoi.flink.model.FlinkTargetType]]
   */
  lazy val InjectedDeployTargetKey = "deploy.target"

  /**
   * Injected config to to mark the cluster spec type
   * value: [[potamoi.flink.model.deploy.ClusterDeployType]]
   */
  lazy val InjectedDeployTypeKey = "deploy.type"

}

sealed trait ClusterSpecResolver {
  type SpecType <: ClusterSpec
  type RevisePipe = SpecType => SpecType

  /**
   * Ensure that the necessary configuration has been set whenever possible.
   */
  def reviseSpec(spec: SpecType): IO[ReviseClusterSpecErr, SpecType]

  protected def internalReviseSpec(spec: SpecType, moreRevisePipe: RevisePipe): IO[ReviseClusterSpecErr, SpecType] = {
    val pipe = {
      removeNotAllowCustomRawConfigs andThen
      completeFlinkBuiltInPlugins andThen
      reviseS3Path andThen
      ensureS3Plugins andThen
      moreRevisePipe
    }
    ZIO.attempt(pipe(spec)).mapError(ReviseClusterSpecErr.apply)
  }

  /**
   * Whether built-in s3 storage support is required.
   */
  def isS3Required(spec: SpecType): Boolean

  protected def internalCheckS3Required(spec: SpecType, moreChecks: => Boolean = false) = {
    lazy val checkStateBackend = spec.props.stateBackend.exists { c =>
      if c.checkpointDir.exists(paths.isS3Path) then true
      else c.savepointDir.exists(paths.isS3Path)
    }
    lazy val checkJmHa         = spec.props.jmHa.exists(c => paths.isS3Path(c.storageDir))
    lazy val checkInjectDeps   = spec.props.udfJars.exists(paths.isS3Path)

    if checkStateBackend then true
    else if checkJmHa then true
    else if checkInjectDeps then true
    else moreChecks
  }

  /**
   * Resolve and convert ClusterSpec to Flink raw config.
   */
  def resolveToFlinkConfig(
      spec: SpecType,
      podTemplateFilePath: String,
      logConfFilePath: String): ZIO[FlinkConf with FsBackendConf, ConvertToFlinkRawConfigErr, Configuration] = {
    for
      genConfig <- genFlinkConfig(spec)
      config     = genConfig
                     .safeSet("kubernetes.pod-template-file.jobmanager", podTemplateFilePath)
                     .safeSet("kubernetes.pod-template-file.taskmanager", podTemplateFilePath)
                     .safeSet("$internal.deployment.config-dir", logConfFilePath)
    yield config
  }

  /**
   * Convert to Flink raw configuration.
   */
  def genFlinkConfig(spec: SpecType): ZIO[FlinkConf with FsBackendConf, ConvertToFlinkRawConfigErr, Configuration]

  protected def internalGenFlinkConfig(spec: SpecType, moreConvert: Configuration => Configuration) = {
    for {
      flinkConf      <- ZIO.service[FlinkConf]
      fallbackS3Prop <- ZIO.service[FsBackendConf].map {
                          // fallback with potamoi S3BackendConf
                          case conf: S3FsBackendConf =>
                            Some(
                              S3AccessProp(
                                endpoint = conf.endpoint,
                                accessKey = conf.accessKey,
                                secretKey = conf.secretKey,
                                pathStyleAccess = Some(conf.accessStyle == S3AccessStyle.PathStyle),
                                sslEnabled = Some(conf.sslEnabled)
                              ))
                          case _                     => None
                        }
      result         <- convertFlinkConfigs(spec, moreConvert, flinkConf, fallbackS3Prop)
    } yield result
  }.mapError(ConvertToFlinkRawConfigErr.apply)

  // ------------------------- Cluster spec revise pipeline start ---------------------------------

  // Filter not allow customized extRawConfigs
  protected val removeNotAllowCustomRawConfigs: RevisePipe = { spec =>
    val revisedProps = spec.props.modify(_.extraProps).using { extraProps =>
      extraProps
        .map(kv => safeTrim(kv._1) -> safeTrim(kv._2))
        .filter(kv => kv._1.nonEmpty && kv._2.nonEmpty)
        .filter(kv => !NotAllowCustomFlinkConfigKeys.contains(kv._1))
    }
    spec.copyIt(props = revisedProps).asInstanceOf[SpecType]
  }

  // Format BuiltInPlugins name
  protected val completeFlinkBuiltInPlugins: RevisePipe = { spec =>
    val revisedProps = spec.props.modify(_.plugins).using { plugins =>
      plugins
        .filterNotBlank()
        .map { name =>
          FlinkPlugin.values
            .find(_.name == name)
            .map(_.jarName(spec.meta.flinkVer))
            .getOrElse(name)
        }
        .toSet
    }
    spec.copyIt(props = revisedProps).asInstanceOf[SpecType]
  }

  // Modify s3 path to s3p schema for checkpoint/savepoint/jm-ha configs.
  protected val reviseS3Path: RevisePipe = { spec =>
    val revisedProps = spec.props
      .modify(_.stateBackend)
      .using {
        _.map(backend =>
          backend.copy(
            checkpointDir = backend.checkpointDir.map(paths.convertS3ToS3pSchema),
            savepointDir = backend.savepointDir.map(paths.convertS3ToS3pSchema)
          ))
      }
      .modify(_.jmHa)
      .using {
        _.map(jmHa => jmHa.copy(storageDir = paths.convertS3ToS3pSchema(jmHa.storageDir)))
      }
    spec.copyIt(props = revisedProps).asInstanceOf[SpecType]
  }

  // Ensure s3 plugins is enabled if necessary.
  protected val ensureS3Plugins: RevisePipe = { spec =>
    val extraS3Plugin = {
      lazy val s3pPluginJarName = FlinkPlugin.S3Presto.jarName(spec.meta.flinkVer)
      if isS3Required(spec) && !spec.props.plugins.contains(s3pPluginJarName) then Some(s3pPluginJarName) else None
    }
    extraS3Plugin match
      case None         => spec
      case Some(plugin) =>
        val revisedProps = spec.props.modify(_.plugins).using(_ ++ Set(plugin))
        spec.copyIt(props = revisedProps).asInstanceOf[SpecType]
  }

  // ------------------------ Flink raw config conversion start ---------------------------------

  private def convertFlinkConfigs(
      spec: SpecType,
      moreConvert: Configuration => Configuration,
      flinkConf: FlinkConf,
      defaultS3Prop: Option[S3AccessProp]): IO[ConvertToFlinkRawConfigErr, Configuration] =
    ZIO
      .attempt {
        Configuration()
          // fixed configs
          .safeSet("execution.target", spec.targetType.rawValue)
          .safeSet("kubernetes.cluster-id", spec.meta.clusterId)
          .safeSet("kubernetes.namespace", spec.meta.namespace)
          .safeSet("kubernetes.container.image", spec.meta.image)
          .safeSet("kubernetes.jobmanager.service-account", spec.meta.k8sAccount.getOrElse(flinkConf.k8sAccount))
          .safeSet("kubernetes.rest-service.exposed.type", spec.props.restExportType.rawValue)
          .safeSet("blob.server.port", 6124)
          .safeSet("taskmanager.rpc.port", 6122)
          // from structured props
          .mergeProp(spec.props.cpu)
          .mergeProp(spec.props.mem)
          .mergeProp(spec.props.par)
          .mergeProp(spec.props.webui)
          .mergeProp(spec.props.restartStg)
          .mergePropOpt(spec.props.stateBackend)
          .mergePropOpt(spec.props.jmHa)
          // s3 configs
          .contra { conf =>
            spec.props.s3 match
              case Some(s3Prop)                                          => conf.mergeMap(s3Prop.mappingS3p ++ s3Prop.mappingS3)
              case None if isS3Required(spec) && defaultS3Prop.isDefined => conf.mergeMap(defaultS3Prop.get.mappingS3p ++ defaultS3Prop.get.mappingS3)
              case _                                                     => conf
          }
          // flink plugins
          .safeSet("containerized.master.env.ENABLE_BUILT_IN_PLUGINS", spec.props.plugins.toSeq)
          .safeSet("containerized.taskmanager.env.ENABLE_BUILT_IN_PLUGINS", spec.props.plugins.toSeq)
          // extension conversion
          .contra(conf => moreConvert(conf))
          // merge with extra custom configs
          .mergeMap(spec.props.extraProps)
          // top level config
          .safeSet(InjectedDeploySourceConf._1, InjectedDeploySourceConf._2)
          .safeSet(InjectedDeployTargetKey, spec.targetType.rawValue)
          .safeSet(InjectedDeployTypeKey, spec.deployType.toString)
      }
      .mapError(ConvertToFlinkRawConfigErr.apply)

}

/**
 * Flink session cluster spec resolver.
 */
object SessionClusterSpecResolver extends ClusterSpecResolver {

  type SpecType = SessionClusterSpec

  def reviseSpec(spec: SessionClusterSpec)     = internalReviseSpec(spec, identity)
  def isS3Required(spec: SessionClusterSpec)   = internalCheckS3Required(spec)
  def genFlinkConfig(spec: SessionClusterSpec) = internalGenFlinkConfig(spec, identity)
}

/**
 * Resolver for Flink application cluster spec based on jar.
 */
object JarAppClusterSpecResolver extends ClusterSpecResolver {

  type SpecType = JarAppClusterSpec

  def reviseSpec(spec: JarAppClusterSpec)   = internalReviseSpec(spec, identity)
  def isS3Required(spec: JarAppClusterSpec) = internalCheckS3Required(spec)

  def genFlinkConfig(spec: JarAppClusterSpec) = internalGenFlinkConfig(
    spec,
    conf =>
      // when jobJar path is pota schema path, replace it with local path on k8s pvc.
      val reviseJarPath =
        if paths.isPotaPath(spec.jobJar) then s"local:///opt/flink/lib/${paths.getFileName(spec.jobJar)}"
        else spec.jobJar
      conf
        .safeSet("pipeline.jars", reviseJarPath)
        .safeSet("pipeline.name", spec.jobName)
        .mergePropOpt(spec.restore)
  )
}

/**
 * Resolver for Flink application cluster spec based on sqls.
 * todo implement it.
 */
//object SqlAppClusterSpecResolver extends ClusterSpecResolver
