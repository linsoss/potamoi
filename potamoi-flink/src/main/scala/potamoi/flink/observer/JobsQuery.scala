package potamoi.flink.observer

import potamoi.flink.model.{Fcid, Fjid, FlinkJobMetrics, FlinkJobOverview, JobState}
import potamoi.flink.FlinkErr
import zio.{IO, Task}
import zio.stream.{Stream, ZStream}

type JobId = String

/**
 * Flink jobs snapshot information query layer.
 */
trait JobsQuery {

  def getOverview(fjid: Fjid): IO[FlinkErr, Option[FlinkJobOverview]]

  def listOverview(fcid: Fcid): IO[FlinkErr, List[FlinkJobOverview]]

  def listAllOverview: Stream[FlinkErr, FlinkJobOverview]

  def listJobId(fcid: Fcid): IO[FlinkErr, List[JobId]]

  def listAllJobId: Stream[FlinkErr, Fjid]

  def getJobState(fjid: Fjid): IO[FlinkErr, Option[JobState]]

  def listJobState(fcid: Fcid): IO[FlinkErr, Map[JobId, JobState]]

  // todo select function

  def getCurMetrics(fjid: Fjid): IO[FlinkErr, Option[FlinkJobMetrics]]

  def listCurMetrics(fcid: Fcid): IO[FlinkErr, List[FlinkJobMetrics]]
  
}
