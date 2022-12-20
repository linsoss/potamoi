package potamoi.flink.observer.query

import potamoi.common.Err
import potamoi.flink.model.{Fjid, FlinkSptTriggerStatus}
import potamoi.flink.storage.{JobMetricsStorage, JobOverviewStorage, JobSnapStorage}
import potamoi.flink.{flinkRest, DataStorageErr, FlinkConf, FlinkErr, FlinkRestEndpointType, FlinkRestErr}
import potamoi.flink.FlinkErr.{ClusterNotFound, WatchTimeout}
import potamoi.flink.FlinkRestErr.{RequestApiErr, TriggerNotFound}
import potamoi.kubernetes.K8sErr
import potamoi.kubernetes.K8sErr.RequestK8sApiErr
import potamoi.times.given_Conversion_ScalaDuration_ZioDuration
import zio.Exit.succeed
import zio.IO
import zio.ZIOAspect.annotated

import scala.concurrent.duration.Duration

/**
 * Flink job observer.
 */
trait FlinkJobQuery {
  def overview: JobOverviewStorage.Query
  def metrics: JobMetricsStorage.Query
  def savepointTrigger: FlinkSavepointTriggerQuery
}

trait FlinkSavepointTriggerQuery {

  /**
   * Get current savepoint trigger status of the flink job.
   */
  def get(fjid: Fjid, triggerId: String): IO[FlinkErr, FlinkSptTriggerStatus]

  /**
   * Watch flink savepoint trigger until it was completed.
   */
  def watch(fjid: Fjid, triggerId: String, timeout: Duration = Duration.Inf): IO[FlinkErr, FlinkSptTriggerStatus]
}

/**
 * Default implementation.
 */
case class FlinkJobQueryLive(flinkConf: FlinkConf, jobSnapStorage: JobSnapStorage, restEndpointQuery: FlinkRestEndpointQuery) extends FlinkJobQuery {
  lazy val overview         = jobSnapStorage.overview
  lazy val metrics          = jobSnapStorage.metrics
  lazy val savepointTrigger = SavepointTriggerQueryLive(flinkConf, restEndpointQuery)

  class SavepointTriggerQueryLive(flinkConf: FlinkConf, restEndpoint: FlinkRestEndpointQuery) extends FlinkSavepointTriggerQuery:
    private given FlinkRestEndpointType = flinkConf.restEndpointTypeInternal

    /**
     * Get current savepoint trigger status of the flink job.
     */
    def get(fjid: Fjid, triggerId: String): IO[FlinkErr, FlinkSptTriggerStatus] = {
      for {
        restUrl <- restEndpoint.getEnsure(fjid.fcid).someOrFail(ClusterNotFound(fjid.fcid)).map(_.chooseUrl)
        rs      <- flinkRest(restUrl).getSavepointOperationStatus(fjid.jobId, triggerId)
      } yield rs
    } @@ annotated(fjid.toAnno :+ "triggerId" -> triggerId: _*)

    /**
     * Watch flink savepoint trigger until it was completed.
     */
    override def watch(
        fjid: Fjid,
        triggerId: String,
        timeout: Duration): IO[FlinkErr, FlinkSptTriggerStatus] = {
      for {
        restUrl <- restEndpoint.getEnsure(fjid.fcid).someOrFail(ClusterNotFound(fjid.fcid)).map(_.chooseUrl)
        rs <- flinkRest(restUrl)
          .getSavepointOperationStatus(fjid.jobId, triggerId)
          .repeatUntilZIO(r => if (r.isCompleted) succeed(true) else succeed(false).delay(flinkConf.tracking.savepointTriggerPolling))
          .timeoutFail(WatchTimeout(timeout))(timeout)
      } yield rs
    } @@ annotated(fjid.toAnno :+ "flink.triggerId" -> triggerId: _*)
}
