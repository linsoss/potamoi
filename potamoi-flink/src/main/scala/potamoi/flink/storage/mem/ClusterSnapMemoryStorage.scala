package potamoi.flink.storage.mem

import potamoi.flink.DataStorageErr
import potamoi.flink.model.*
import potamoi.flink.storage.*
import zio.{stream, IO, Ref, UIO, ULayer, ZLayer}
import zio.stream.{Stream, ZSink, ZStream}

import scala.collection.mutable

/**
 * Flink cluster snapshot storage in-memory implementation.
 */
object ClusterSnapMemoryStorage:
  def instance: UIO[ClusterSnapStorage] =
    for {
      ovRef              <- Ref.make(mutable.Map.empty[Fcid, FlinkClusterOverview])
      jmMetricRef        <- Ref.make(mutable.Map.empty[Fcid, FlinkJmMetrics])
      tmRef              <- Ref.make(mutable.Map.empty[Ftid, FlinkTmDetail])
      tmDetailMetricsRef <- Ref.make(mutable.Map.empty[Ftid, FlinkTmMetrics])
    } yield new ClusterSnapStorage:
      lazy val overview: ClusterOverviewStorage = ClusterOverviewMemoryStorage(ovRef)
      lazy val jmMetrics: JmMetricsStorage      = JmMetricsMemoryStorage(jmMetricRef)
      lazy val tmDetail: TmDetailStorage        = TmDetailMemoryStorage(tmRef)
      lazy val tmMetrics: TmMetricStorage       = TmMetricMemoryStorage(tmDetailMetricsRef)

class ClusterOverviewMemoryStorage(ref: Ref[mutable.Map[Fcid, FlinkClusterOverview]]) extends ClusterOverviewStorage:
  private val stg                                                       = MapBasedStg(ref)
  def get(fcid: Fcid): IO[DataStorageErr, Option[FlinkClusterOverview]] = stg.get(fcid)
  def listAll: Stream[DataStorageErr, FlinkClusterOverview]             = stg.streamValues
  def put(ov: FlinkClusterOverview): IO[DataStorageErr, Unit]           = stg.put(ov.fcid, ov)
  def rm(fcid: Fcid): IO[DataStorageErr, Unit]                          = stg.delete(fcid)

class JmMetricsMemoryStorage(ref: Ref[mutable.Map[Fcid, FlinkJmMetrics]]) extends JmMetricsStorage:
  private val stg                                                 = MapBasedStg(ref)
  def get(fcid: Fcid): IO[DataStorageErr, Option[FlinkJmMetrics]] = stg.get(fcid)
  def put(metric: FlinkJmMetrics): IO[DataStorageErr, Unit]       = stg.put(Fcid(metric.clusterId, metric.namespace), metric)
  def rm(fcid: Fcid): IO[DataStorageErr, Unit]                    = stg.delete(fcid)

class TmDetailMemoryStorage(ref: Ref[mutable.Map[Ftid, FlinkTmDetail]]) extends TmDetailStorage:
  private val stg                                                = MapBasedStg(ref)
  def put(tm: FlinkTmDetail): IO[DataStorageErr, Unit]           = stg.put(tm.ftid, tm)
  def putAll(tm: List[FlinkTmDetail]): IO[DataStorageErr, Unit]  = stg.putAll(tm.map(tm => tm.ftid -> tm).toMap)
  def rm(ftid: Ftid): IO[DataStorageErr, Unit]                   = stg.delete(ftid)
  def rm(fcid: Fcid): IO[DataStorageErr, Unit]                   = stg.deleteByKey(_.fcid == fcid)
  def get(ftid: Ftid): IO[DataStorageErr, Option[FlinkTmDetail]] = stg.get(ftid)
  def list(fcid: Fcid): IO[DataStorageErr, List[FlinkTmDetail]]  = stg.getByKey(_.fcid == fcid)
  def listAll: Stream[DataStorageErr, FlinkTmDetail]             = stg.streamValues
  def listTmId(fcid: Fcid): IO[DataStorageErr, List[Ftid]]       = stg.getPartByKey(_.fcid == fcid, _.ftid)
  def listAllTmId: Stream[DataStorageErr, Ftid]                  = stg.streamValues.map(_.ftid)

class TmMetricMemoryStorage(ref: Ref[mutable.Map[Ftid, FlinkTmMetrics]]) extends TmMetricStorage:
  private val stg                                                 = MapBasedStg(ref)
  def put(metric: FlinkTmMetrics): IO[DataStorageErr, Unit]       = stg.put(Ftid(metric.clusterId, metric.namespace, metric.tmId), metric)
  def rm(ftid: Ftid): IO[DataStorageErr, Unit]                    = stg.delete(ftid)
  def rm(fcid: Fcid): IO[DataStorageErr, Unit]                    = stg.deleteByKey(_.fcid == fcid)
  def get(ftid: Ftid): IO[DataStorageErr, Option[FlinkTmMetrics]] = stg.get(ftid)
  def list(fcid: Fcid): IO[DataStorageErr, List[FlinkTmMetrics]]  = stg.getByKey(_.fcid == fcid)
  def listTmId(fcid: Fcid): IO[DataStorageErr, List[Ftid]]        = stg.getKeys.map(_.filter(_.fcid == fcid))
