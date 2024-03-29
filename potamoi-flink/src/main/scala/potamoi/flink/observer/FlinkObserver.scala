package potamoi.flink.observer

import com.coralogix.zio.k8s.model.apps.v1.DeploymentSpec
import com.coralogix.zio.k8s.model.core.v1.{PodSpec, ServiceSpec}
import potamoi.akka.AkkaMatrix
import potamoi.flink.{FlinkConf, FlinkErr, FlinkRestEndpointRetriever}
import potamoi.flink.model.{Fcid, Fjid}
import potamoi.flink.observer.query.*
import potamoi.flink.observer.tracker.{TrackerManager, TrackManager}
import potamoi.flink.storage.FlinkDataStorage
import potamoi.kubernetes.K8sErr.RequestK8sApiErr
import potamoi.kubernetes.K8sOperator
import potamoi.logger.LogConf
import potamoi.EarlyLoad
import potamoi.flink.model.snapshot.{FlinkRestSvcEndpoint, FlinkSptTriggerStatus}
import zio.{IO, Scope, URIO, ZIO, ZLayer}
import zio.stream.Stream

import scala.concurrent.duration.Duration

/**
 * Flink cluster on kubernetes observer.
 */
trait FlinkObserver {

  /**
   * Flink cluster tracker management.
   */
  def manager: TrackManager

  /**
   * Flink cluster rest endpoint info query.
   */
  def restEndpoint: FlinkRestEndpointQuery

  /**
   * Flink clusters information snapshot query.
   */
  def cluster: FlinkClusterQuery

  /**
   * Flink jobs snapshot query.
   */
  def job: FlinkJobQuery

  /**
   * Flink cluster kubernetes info snapshot query.
   */
  def k8s: FlinkK8sRefQuery

}

object FlinkObserver extends EarlyLoad[FlinkObserver] {

  val live: ZLayer[FlinkDataStorage with K8sOperator with AkkaMatrix with FlinkConf with LogConf, Throwable, FlinkObserver] =
    ZLayer {
      for {
        logConf           <- ZIO.service[LogConf]
        flinkConf         <- ZIO.service[FlinkConf]
        actorCradle       <- ZIO.service[AkkaMatrix]
        k8sOperator       <- ZIO.service[K8sOperator]
        snapStorage       <- ZIO.service[FlinkDataStorage]
        eptRetriever      <- FlinkRestEndpointRetriever.make(k8sOperator.client)
        restEndpointQuery <- FlinkRestEndpointQuery.make(snapStorage.restEndpoint, eptRetriever)
        clusterQuery      <- FlinkClusterQuery.make(snapStorage.cluster)
        jobQuery          <- FlinkJobQuery.make(flinkConf, snapStorage.job, restEndpointQuery)
        k8sRefQuery       <- FlinkK8sRefQuery.make(snapStorage.k8sRef, k8sOperator)
        trackerManager    <- TrackerManager.make(logConf, flinkConf, actorCradle, snapStorage, eptRetriever, k8sOperator)
      } yield new FlinkObserver:
        val manager           = trackerManager // should not be delayed loading!
        lazy val restEndpoint = restEndpointQuery
        lazy val cluster      = clusterQuery
        lazy val job          = jobQuery
        lazy val k8s          = k8sRefQuery
    }

  override def active: URIO[FlinkObserver, Unit] = ZIO.service[FlinkObserver].unit
}
