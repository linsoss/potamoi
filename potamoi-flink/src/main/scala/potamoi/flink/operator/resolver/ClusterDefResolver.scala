package potamoi.flink.operator.resolver

import cats.Eval
import org.apache.flink.configuration.Configuration
import potamoi.common.CollectionExtension.filterNotBlank
import potamoi.flink.FlinkConf
import potamoi.flink.FlinkConfigExtension.{ConfigurationPF, given}
import potamoi.flink.ResolveFlinkClusterDefErr.*
import potamoi.flink.model.*
import potamoi.flink.model.FlinkPlugin.{S3Hadoop, S3Presto}
import potamoi.flink.model.FlinkPlugins.S3aPlugins
import potamoi.flink.operator.resolver.ClusterDefResolver.notAllowCustomRawConfKeys
import potamoi.fs.S3Conf
import potamoi.fs.paths.*
import potamoi.syntax.{contra, safeTrim}
import zio.{IO, ZIO}

/**
 * Flink cluster definition resolver for [[FlinkClusterDef]].
 */
sealed trait ClusterDefResolver[ClusterDef <: FlinkClusterDef[ClusterDef]] {

  /**
   * Whether built-in s3 storage support is required.
   */
  def isS3Required(clusterDef: ClusterDef): Boolean

  /**
   * Ensure that the necessary configuration has been set whenever possible.
   */
  def revise(clusterDef: ClusterDef): IO[ReviseClusterDefErr, ClusterDef]

  /**
   * Convert to Flink raw configuration.
   */
  def toFlinkRawConfig(clusterDef: ClusterDef, flinkConf: FlinkConf, s3Conf: S3Conf): IO[ConvertToFlinkRawConfigErr, Configuration]

  /**
   * Check whether S3 support is required.
   */
  protected def checkS3Required(clusterDef: ClusterDef, moreChecks: Eval[Boolean]): Boolean = {
    lazy val checkStateBackend = clusterDef.stateBackend.exists { c =>
      if (c.checkpointDir.exists(isS3Path)) true
      else c.savepointDir.exists(isS3Path)
    }
    lazy val checkJmHa       = clusterDef.jmHa.exists(c => isS3Path(c.storageDir))
    lazy val checkInjectDeps = clusterDef.injectedDeps.exists(isS3Path)

    if (checkStateBackend) true
    else if (checkJmHa) true
    else if (checkInjectDeps) true
    else moreChecks.value
  }

  protected type RevisePipe = ClusterDef => ClusterDef

  /**
   * Ensure that the necessary configuration has been set whenever possible.
   */
  protected def reviseDefinition(clusterDef: ClusterDef, moreRevisePipe: RevisePipe = identity): IO[ReviseClusterDefErr, ClusterDef] = {
    val pipe = removeNotAllowCustomRawConfigs andThen
      completeBuiltInPlugins andThen
      reviseS3Path andThen
      ensureS3Plugins andThen
      ensureHdfsPlugins andThen
      moreRevisePipe
    ZIO.attempt(pipe(clusterDef)).mapError(ReviseClusterDefErr.apply)
  }

  // filter not allow customized extRawConfigs.
  private val removeNotAllowCustomRawConfigs: RevisePipe = clDef =>
    clDef.copyExtRawConfigs(
      clDef.extRawConfigs
        .map(kv => safeTrim(kv._1) -> safeTrim(kv._2))
        .filter(kv => kv._1.nonEmpty && kv._2.nonEmpty)
        .filter(kv => !notAllowCustomRawConfKeys.contains(kv._1))
    )

  // format BuiltInPlugins fields.
  private val completeBuiltInPlugins: RevisePipe = clDef =>
    clDef.copyBuiltInPlugins(
      clDef.builtInPlugins
        .filterNotBlank()
        .map { name => FlinkPlugin.values.find(_.name == name).map(_.jarName(clDef.flinkVer)).getOrElse(name) }
        .toSet
    )

  // modify s3 path to s3p schema
  private val reviseS3Path: RevisePipe = clDef =>
    clDef
      .copyStateBackend(
        clDef.stateBackend.map(conf =>
          conf.copy(
            checkpointDir = conf.checkpointDir.map(reviseToS3pSchema),
            savepointDir = conf.savepointDir.map(reviseToS3pSchema)
          ))
      )
      .copyJmHa(
        clDef.jmHa.map(conf =>
          conf.copy(
            storageDir = reviseToS3pSchema(conf.storageDir)
          ))
      )
      .copyInjectDeps(
        clDef.injectedDeps.map(reviseToS3pSchema)
      )

  // ensure s3 plugins is enabled if necessary.
  private val ensureS3Plugins: RevisePipe = { clDef =>
    val extBuildInS3PluginJar = {
      val s3pJar = S3Presto.jarName(clDef.flinkVer)
      if (isS3Required(clDef) && !clDef.builtInPlugins.contains(s3pJar)) s3pJar else ""
    }
    val extJobS3PluginJar = {
      if (clDef.s3.isEmpty) ""
      else {
        val s3aJars = S3aPlugins.map(_.jarName(clDef.flinkVer))
        if ((clDef.builtInPlugins & s3aJars).isEmpty) S3Hadoop.jarName(clDef.flinkVer) else ""
      }
    }
    val extraPluginJars = Vector(extBuildInS3PluginJar, extJobS3PluginJar).filter(_.nonEmpty)
    if (extraPluginJars.nonEmpty) clDef.copyBuiltInPlugins(clDef.builtInPlugins ++ extraPluginJars) else clDef
  }

  // ensure hadoop plugins is enabled if necessary.
  private val ensureHdfsPlugins: RevisePipe = identity

  /**
   * Convert to Flink raw configuration.
   */
  protected def convertToFlinkConfig(
      clusterDef: ClusterDef,
      flinkConf: FlinkConf,
      s3Conf: S3Conf,
      moreInject: ConfigurationPF => ConfigurationPF = identity): IO[ConvertToFlinkRawConfigErr, Configuration] =
    ZIO
      .attempt {
        Configuration()
          // inject inner raw configs
          .append("execution.target", clusterDef.execType.rawValue)
          .append("kubernetes.cluster-id", clusterDef.clusterId)
          .append("kubernetes.namespace", clusterDef.namespace)
          .append("kubernetes.container.image", clusterDef.image)
          .append("kubernetes.jobmanager.service-account", clusterDef.k8sAccount.getOrElse(flinkConf.k8sAccount))
          .append("kubernetes.rest-service.exposed.type", clusterDef.restExportType.rawValue)
          .append("blob.server.port", 6124)
          .append("taskmanager.rpc.port", 6122)
          .append(clusterDef.cpu)
          .append(clusterDef.mem)
          .append(clusterDef.par)
          .append(clusterDef.webui)
          .append(clusterDef.restartStg)
          .append(clusterDef.stateBackend)
          .append(clusterDef.jmHa)
          // s3 raw configs if necessary
          .pipe { conf =>
            // using s3p raw config and standard s3 raw config to resolve savepoint/checkpoint trigger issues.
            val buildInS3Conf = if isS3Required(clusterDef) then S3AccessConf(s3Conf).contra(f => f.mappingS3p ++ f.mappingS3) else Vector.empty
            // using s3a config for custom job.
            val jobS3Conf = clusterDef.s3.map(_.mappingS3a).getOrElse(Vector.empty)
            (buildInS3Conf ++ jobS3Conf).foldLeft(conf)((ac, c) => ac.append(c._1, c._2))
          }
          // built-in plugins raw configs
          .pipeWhen(clusterDef.builtInPlugins.nonEmpty) { conf =>
            conf
              .append("containerized.master.env.ENABLE_BUILT_IN_PLUGINS", clusterDef.builtInPlugins)
              .append("containerized.taskmanager.env.ENABLE_BUILT_IN_PLUGINS", clusterDef.builtInPlugins)
          }
          .pipe(moreInject)
          // override extra raw configs
          .merge(clusterDef.extRawConfigs)
          .value
      }
      .mapError(ConvertToFlinkRawConfigErr.apply)
}

object ClusterDefResolver {
  val session     = SessClusterDefResolver
  val application = AppClusterDefResolver

  /**
   * Flink raw configuration keys that are not allowed to be customized by users.
   */
  lazy val notAllowCustomRawConfKeys: Vector[String] = Vector(
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
    "$internal.application.program-args",
  )
}

/**
 * Flink session cluster definition resolver.
 */
object SessClusterDefResolver extends ClusterDefResolver[FlinkSessClusterDef] {
  override def isS3Required(clusterDef: FlinkSessClusterDef): Boolean =
    checkS3Required(clusterDef, Eval.now(false))

  override def revise(clusterDef: FlinkSessClusterDef): IO[ReviseClusterDefErr, FlinkSessClusterDef] =
    reviseDefinition(clusterDef)

  override def toFlinkRawConfig(clusterDef: FlinkSessClusterDef, flinkConf: FlinkConf, s3Conf: S3Conf): IO[ConvertToFlinkRawConfigErr, Configuration] =
    convertToFlinkConfig(clusterDef, flinkConf, s3Conf)
}

/**
 * Flink application cluster definition resolver.
 */
object AppClusterDefResolver extends ClusterDefResolver[FlinkAppClusterDef] {
  override def isS3Required(clusterDef: FlinkAppClusterDef): Boolean =
    checkS3Required(clusterDef, Eval.later(isS3Path(clusterDef.jobJar)))

  override def revise(clusterDef: FlinkAppClusterDef): IO[ReviseClusterDefErr, FlinkAppClusterDef] =
    reviseDefinition(clusterDef, clDef => clDef.copy(jobJar = reviseToS3pSchema(clDef.jobJar)))

  override def toFlinkRawConfig(clusterDef: FlinkAppClusterDef, flinkConf: FlinkConf, s3Conf: S3Conf): IO[ConvertToFlinkRawConfigErr, Configuration] =
    convertToFlinkConfig(
      clusterDef,
      flinkConf,
      s3Conf,
      rawConf =>
        // when jobJar path is s3 path, replace with pvc local path.
        val reviseJarPath = if isS3Path(clusterDef.jobJar) then s"local:///opt/flink/lib/${clusterDef.jobJar.split('/').last}" else clusterDef.jobJar
        rawConf
          .append("pipeline.jars", reviseJarPath)
          .append(clusterDef.restore)
        clusterDef.jobName match
          case Some(name) => rawConf.append("pipeline.name", name)
          case None       => rawConf
    )
}
