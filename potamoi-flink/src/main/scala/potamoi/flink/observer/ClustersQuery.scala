package potamoi.flink.observer

import potamoi.flink.model.{Fcid, FlinkClusterOverview, FlinkJmMetrics, FlinkTmDetail, FlinkTmMetrics, Ftid}
import potamoi.flink.FlinkErr
import zio.IO
import zio.stream.Stream

/**
 * Flink cluster snapshot information query layer.
 */
trait ClustersQuery {

  def getOverview(fcid: Fcid): IO[FlinkErr, Option[FlinkClusterOverview]]
  def listOverview: IO[FlinkErr, List[FlinkClusterOverview]]
  def listAllOverview: Stream[FlinkErr, FlinkClusterOverview]

  def listTmIds(fcid: Fcid): IO[FlinkErr, List[String]]
  def getTmDetail(ftid: Ftid): IO[FlinkErr, Option[FlinkTmDetail]]
  def listTmDetails(fcid: Fcid): IO[FlinkErr, List[FlinkTmDetail]]

  def getCurJmMetrics(fcid: Fcid): IO[FlinkErr, Option[FlinkJmMetrics]]
  def getCurTmMetrics(ftid: Ftid): IO[FlinkErr, Option[FlinkTmMetrics]]
  def listCurTmMetrics(fcid: Fcid): IO[FlinkErr, List[FlinkTmMetrics]]

}
