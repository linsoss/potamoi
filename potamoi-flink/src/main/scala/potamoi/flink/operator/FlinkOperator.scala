package potamoi.flink.operator

import potamoi.flink.FlinkConf
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.storage.FlinkDataStorage
import potamoi.fs.*
import potamoi.kubernetes.K8sOperator
import potamoi.BaseConf
import zio.{ZIO, ZLayer}

/**
 * Flink on Kubernetes Operator.
 */
trait FlinkOperator {
  def unify: FlinkClusterUnifyOperator
  def session: FlinkSessionModeOperator
  def application: FlinkClusterAppModeOperator
  def restProxy: FlinkRestProxyOperator
}

object FlinkOperator {

  val live = ZLayer {
    for {
      flinkConf      <- ZIO.service[FlinkConf]
      baseConf       <- ZIO.service[BaseConf]
      fileServerConf <- ZIO.service[FileServerConf]
      fsBackendConf  <- ZIO.service[FsBackendConf]
      remoteFs       <- ZIO.service[RemoteFsOperator]
      flinkObserver  <- ZIO.service[FlinkObserver]
      dataStorage    <- ZIO.service[FlinkDataStorage]
      k8sOperator    <- ZIO.service[K8sOperator]
    } yield new FlinkOperator:
      lazy val unify       = FlinkClusterUnifyOperatorImpl(flinkConf, k8sOperator, flinkObserver)
      lazy val session     = FlinkSessionModeOperatorImpl(flinkConf, baseConf, fileServerConf, k8sOperator, fsBackendConf, remoteFs, flinkObserver)
      lazy val application = FlinkClusterAppModeOperatorImpl(flinkConf, baseConf, fileServerConf, k8sOperator, fsBackendConf, flinkObserver)
      lazy val restProxy   = FlinkRestProxyOperatorImpl(dataStorage)
  }

}
