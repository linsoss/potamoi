package potamoi.flink.model.deploy

import potamoi.codecs
import potamoi.flink.{FlinkMajorVer, FlinkVersion}
import potamoi.flink.model.{Fcid, FlinkTargetType}
import potamoi.flink.model.deploy.RestartStrategyProp.NonRestart
import potamoi.flink.model.deploy.RestExportType.ClusterIP
import potamoi.flink.model.deploy.RestExportTypes.given
import zio.json.{jsonField, JsonCodec}

/**
 * Flink cluster deployment specification.
 */
sealed trait ClusterSpec {
  def meta: ClusterMeta
  def props: ClusterProps
  def targetType: FlinkTargetType
  def deployType: ClusterDeployType

  type SubType <: ClusterSpec
  def copyIt(meta: ClusterMeta = meta, props: ClusterProps = props): SubType
}

/**
 * Flink K8s session mode cluster specification.
 */
@jsonField("session")
case class SessionClusterSpec(
    meta: ClusterMeta,
    props: ClusterProps = ClusterProps(webui = WebUIProp(enableSubmit = true, enableCancel = true)))
    extends ClusterSpec
    derives JsonCodec {

  val targetType = FlinkTargetType.K8sSession
  val deployType = ClusterDeployType.Session

  type SubType = SessionClusterSpec
  def copyIt(meta: ClusterMeta, props: ClusterProps): SubType = this.copy(meta = meta, props = props)
}

/**
 * Flink K8s application mode cluster specification.
 */
sealed trait AppClusterSpec extends ClusterSpec {
  def jobName: Option[String]
  def restore: Option[SavepointRestoreProp]
  def appMain: Option[String]
  def appArgs: List[String]
  val targetType = FlinkTargetType.K8sApplication
}

/**
 * Flink K8s application mode cluster specification based on jar.
 */
@jsonField("jar-app")
case class JarAppClusterSpec(
    meta: ClusterMeta,
    props: ClusterProps = ClusterProps(webui = WebUIProp(enableSubmit = false, enableCancel = true)),
    jobJar: String,
    jobName: Option[String] = None,
    appMain: Option[String] = None,
    appArgs: List[String] = List.empty,
    restore: Option[SavepointRestoreProp] = None)
    extends AppClusterSpec
    derives JsonCodec {

  val deployType = ClusterDeployType.JarApp

  type SubType = JarAppClusterSpec
  def copyIt(meta: ClusterMeta, props: ClusterProps): SubType = this.copy(meta = meta, props = props)
}

/**
 * Flink K8s application mode cluster specification based on sql.
 * todo implementation it
 */
@jsonField("sql-app")
case class SqlAppClusterSpec(
    meta: ClusterMeta,
    props: ClusterProps = ClusterProps(webui = WebUIProp(enableSubmit = false, enableCancel = true)),
    sqls: String,
    jobName: Option[String] = None,
    restore: Option[SavepointRestoreProp] = None)
    extends AppClusterSpec
    derives JsonCodec {

  val appMain: Option[String] = None
  val appArgs: List[String]   = List.empty
  val deployType              = ClusterDeployType.SqlApp

  type SubType = SqlAppClusterSpec
  def copyIt(meta: ClusterMeta, props: ClusterProps): SubType = this.copy(meta = meta, props = props)
}
