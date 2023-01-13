package potamoi.flink.observer.query

import potamoi.flink.*
import potamoi.flink.model.{Fjid, FlinkPipeOprState, FlinkSptTriggerStatus}
import potamoi.flink.storage.{JobMetricsStorage, JobOverviewStorage, JobSnapStorage}
import potamoi.flink.FlinkErr.{ClusterNotFound, WatchTimeout}
import potamoi.flink.FlinkRestErr.{JobNotFound, RequestApiErr, TriggerNotFound}
import potamoi.kubernetes.K8sErr
import potamoi.kubernetes.K8sErr.RequestK8sApiErr
import potamoi.times.given_Conversion_ScalaDuration_ZioDuration
import zio.{IO, Ref, ZIO}
import zio.Schedule.{recurWhile, spaced}
import zio.ZIOAspect.annotated
import zio.stream.{Stream, ZPipeline, ZStream}

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
   * Watch flink savepoint trigger status until it was completed.
   */
  def watch(fjid: Fjid, triggerId: String): Stream[FlinkErr, FlinkSptTriggerStatus]

  /**
   * Wait flink savepoint trigger completed result.
   */
  def waitComplete(fjid: Fjid, triggerId: String, timeout: Duration = Duration.Inf): IO[FlinkErr, FlinkSptTriggerStatus]

}

/**
 * Default implementation.
 */
case class FlinkJobQueryLive(flinkConf: FlinkConf, jobSnapStorage: JobSnapStorage, restEndpointQuery: FlinkRestEndpointQuery) extends FlinkJobQuery {
  lazy val overview         = jobSnapStorage.overview
  lazy val metrics          = jobSnapStorage.metrics
  lazy val savepointTrigger = SavepointTriggerQueryLive(flinkConf, restEndpointQuery)

  class SavepointTriggerQueryLive(flinkConf: FlinkConf, restEndpoint: FlinkRestEndpointQuery) extends FlinkSavepointTriggerQuery:

    private given FlinkRestEndpointType   = flinkConf.restEndpointTypeInternal
    private val sptPollInterval: Duration = flinkConf.tracking.savepointTriggerPolling

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
     * Watch flink savepoint trigger status until it was completed.
     */
    override def watch(fjid: Fjid, triggerId: String): Stream[FlinkErr, FlinkSptTriggerStatus] = {
      def watchEffect(restUrl: String, preState: Ref[Option[FlinkPipeOprState]]) =
        ZStream
          .fromZIO(flinkRest(restUrl).getSavepointOperationStatus(fjid.jobId, triggerId))
          .repeat(spaced(sptPollInterval))
          .filterZIO { status =>
            for {
              pre <- preState.get
              shouldDrop = pre.contains(status.state)
              _ <- preState.set(Some(status.state))
            } yield !shouldDrop
          }
          .takeUntil(_.isCompleted)
      ZStream
        .fromZIO {
          for {
            restUrl  <- restEndpoint.getEnsure(fjid.fcid).someOrFail(ClusterNotFound(fjid.fcid)).map(_.chooseUrl)
            preState <- Ref.make[Option[FlinkPipeOprState]](None)
          } yield restUrl -> preState
        }
        .flatMap { case (restUrl, preState) => watchEffect(restUrl, preState) }
    }

    /**
     * Watch flink savepoint trigger until it was completed.
     */
    override def waitComplete(
        fjid: Fjid,
        triggerId: String,
        timeout: Duration): IO[ClusterNotFound | JobNotFound | TriggerNotFound | WatchTimeout | FlinkErr, FlinkSptTriggerStatus] = {
      for {
        restUrl <- restEndpoint.getEnsure(fjid.fcid).someOrFail(ClusterNotFound(fjid.fcid)).map(_.chooseUrl)
        rs <- flinkRest(restUrl)
          .getSavepointOperationStatus(fjid.jobId, triggerId)
          .repeatUntilZIO(r => if (r.isCompleted) ZIO.succeed(true) else ZIO.succeed(false).delay(flinkConf.tracking.savepointTriggerPolling))
          .timeoutFail(WatchTimeout(timeout))(timeout)
      } yield rs
    } @@ annotated(fjid.toAnno :+ "flink.triggerId" -> triggerId: _*)
}
