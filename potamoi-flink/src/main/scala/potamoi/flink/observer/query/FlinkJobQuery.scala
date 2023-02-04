package potamoi.flink.observer.query

import potamoi.flink.*
import potamoi.flink.model.Fjid
import potamoi.flink.storage.{JobMetricsStorage, JobOverviewStorage, JobSnapStorage}
import potamoi.flink.FlinkErr.{ClusterNotFound, WatchTimeout}
import potamoi.flink.FlinkRestErr.{JobNotFound, RequestApiErr, TriggerNotFound}
import potamoi.flink.model.snapshot.{FlinkPipeOprState, FlinkSptTriggerStatus}
import potamoi.flink.observer.query.FlinkJobQuery.GetSavepointErr
import potamoi.flink.observer.query.FlinkRestEndpointQuery.GetRestEptErr
import potamoi.kubernetes.K8sErr
import potamoi.kubernetes.K8sErr.RequestK8sApiErr
import potamoi.times.given_Conversion_ScalaDuration_ZIODuration
import potamoi.zios.someOrFailUnion
import zio.{IO, Ref, UIO, ZIO}
import zio.Schedule.{recurWhile, spaced}
import zio.ZIOAspect.annotated
import zio.stream.{Stream, ZPipeline, ZStream}

import scala.concurrent.duration.Duration

/**
 * Flink job observer.
 */
trait FlinkJobQuery {

  /**
   * Flink jobs overview snapshot query.
   */
  def overview: JobOverviewStorage.Query

  /**
   * Flink jobs metrics query.
   */
  def metrics: JobMetricsStorage.Query

  /**
   * Flink jobs savepoint trigger query.
   */
  def savepointTrigger: FlinkSavepointTriggerQuery
}

trait FlinkSavepointTriggerQuery {

  /**
   * Get current savepoint trigger status of the flink job.
   */
  def get(fjid: Fjid, triggerId: String): IO[GetSavepointErr, FlinkSptTriggerStatus]

  /**
   * Watch flink savepoint trigger status until it was completed.
   */
  def watch(fjid: Fjid, triggerId: String): Stream[GetSavepointErr, FlinkSptTriggerStatus]
}

object FlinkJobQuery {

  type GetSavepointErr = (GetRestEptErr | ClusterNotFound | TriggerNotFound | RequestApiErr) with FlinkErr

  def make(flinkConf: FlinkConf, jobSnapStorage: JobSnapStorage, restEndpointQuery: FlinkRestEndpointQuery): UIO[FlinkJobQuery] =
    ZIO.succeed(new FlinkJobQuery {
      lazy val overview         = jobSnapStorage.overview
      lazy val metrics          = jobSnapStorage.metrics
      lazy val savepointTrigger = SavepointTriggerQueryImpl(flinkConf, restEndpointQuery)
    })
}

class SavepointTriggerQueryImpl(flinkConf: FlinkConf, restEndpoint: FlinkRestEndpointQuery) extends FlinkSavepointTriggerQuery {

  private given FlinkRestEndpointType   = flinkConf.restEndpointTypeInternal
  private val sptPollInterval: Duration = flinkConf.tracking.savepointTriggerPolling

  /**
   * Get current savepoint trigger status of the flink job.
   */
  def get(fjid: Fjid, triggerId: String): IO[GetSavepointErr, FlinkSptTriggerStatus] = {
    val ef: IO[GetSavepointErr, FlinkSptTriggerStatus] =
      for {
        restUrl <- restEndpoint
                     .getEnsure(fjid.fcid)
                     .someOrFailUnion(ClusterNotFound(fjid.fcid))
                     .map(_.chooseUrl)
        rs      <- flinkRest(restUrl).getSavepointOperationStatus(fjid.jobId, triggerId)
      } yield rs
    ef @@ annotated(fjid.toAnno :+ "triggerId" -> triggerId: _*)
  }

  /**
   * Watch flink savepoint trigger status until it was completed.
   */
  override def watch(fjid: Fjid, triggerId: String): Stream[GetSavepointErr, FlinkSptTriggerStatus] = {
    for {
      restUrl <- ZStream.fromZIO {
                   restEndpoint
                     .getEnsure(fjid.fcid)
                     .someOrFailUnion(ClusterNotFound(fjid.fcid))
                     .map(_.chooseUrl)
                 }
      status  <- ZStream
                   .fromZIO(flinkRest(restUrl).getSavepointOperationStatus(fjid.jobId, triggerId))
                   .repeat(spaced(sptPollInterval))
                   .changes
                   .takeUntil(_.isCompleted)
    } yield status
  }

}
