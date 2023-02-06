package potamoi.flink.operator

import potamoi.flink.{FlinkConf, FlinkErr, JobId}
import potamoi.flink.model.deploy.{SessionClusterSpec, SessionJobSpec}
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.FlinkErr.ClusterAlreadyExist
import potamoi.fs.refactor.RemoteFsOperator
import potamoi.kubernetes.K8sOperator
import zio.{IO, ZIO}
import zio.ZIO.succeed

/**
 * Flink session mode cluster operator.
 */
trait FlinkSessionClusterOperator extends FlinkUnifyClusterOperator {

  /**
   * Deploy Flink session cluster.
   */
  def deployCluster(definition: SessionClusterSpec): IO[FlinkErr, Unit]

  /**
   * Submit job to Flink session cluster.
   */
  def submitJob(definition: SessionJobSpec): IO[FlinkErr, JobId]

}

/**
 * Default implementation.
 */
class FlinkSessionClusterOperatorImpl(
    flinkConf: FlinkConf,
    k8sOperator: K8sOperator,
    remoteFs: RemoteFsOperator,
    observer: FlinkObserver)
    extends FlinkUnifyClusterOperatorImpl(flinkConf, k8sOperator, observer) with FlinkSessionClusterOperator {

  /**
   * Deploy Flink session cluster.
   */
  override def deployCluster(spec: SessionClusterSpec): IO[FlinkErr, Unit] = {
    for {
      fcid    <- ZIO.succeed(spec.meta.fcid)
      // checking if fcid is occupied by remote k8s
      isAlive <- isRemoteClusterAlive(fcid)
      _       <- ZIO.fail(ClusterAlreadyExist(fcid)).when(isAlive)

    } yield ()
  }

  /**
   * Submit job to Flink session cluster.
   */
  override def submitJob(definition: SessionJobSpec): IO[FlinkErr, JobId] = ???
}
