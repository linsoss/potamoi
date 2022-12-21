package potamoi.flink.operator

import potamoi.flink.FlinkConf
import potamoi.flink.observer.FlinkObserver
import potamoi.fs.{S3Conf, S3Operator}
import potamoi.kubernetes.K8sOperator
import zio.{ZIO, ZLayer}

/**
 * Flink on Kubernetes Operator.
 */
trait FlinkOperator {
  def unify: FlinkClusterUnifyOperator
  def session: FlinkSessClusterOperator
  def application: FlinkAppClusterOperator
//  def restProxy: FlinkRestProxyOperator
}

object FlinkOperator {

  val live = ZLayer {
    for {
      flinkConf     <- ZIO.service[FlinkConf]
      s3Conf        <- ZIO.service[S3Conf]
      flinkObserver <- ZIO.service[FlinkObserver]
      s3Operator    <- ZIO.service[S3Operator]
      k8sOperator   <- ZIO.service[K8sOperator]
    } yield new FlinkOperator:
      lazy val unify       = FlinkClusterUnifyOperatorLive(flinkConf, k8sOperator, flinkObserver)
      lazy val session     = FlinkSessClusterOperatorLive(flinkConf, s3Conf, k8sOperator, s3Operator, flinkObserver)
      lazy val application = FlinkAppClusterOperatorLive(flinkConf, s3Conf, k8sOperator, s3Operator, flinkObserver)
  }
  
}
