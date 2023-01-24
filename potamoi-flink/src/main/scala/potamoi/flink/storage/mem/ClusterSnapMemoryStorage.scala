package potamoi.flink.storage.mem

import potamoi.flink.FlinkDataStoreErr
import potamoi.flink.model.*
import potamoi.flink.storage.*
import zio.{stream, IO, Ref, UIO, ULayer, ZLayer}
import zio.stream.{Stream, ZSink, ZStream}

import scala.collection.mutable

/**
 * Flink cluster snapshot storage in-memory implementation.
 */
object ClusterSnapMemoryStorage:
  def make: UIO[ClusterSnapStorage] =
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
  def get(fcid: Fcid): IO[FlinkDataStoreErr, Option[FlinkClusterOverview]] = stg.get(fcid)
  def listAll: Stream[FlinkDataStoreErr, FlinkClusterOverview]             = stg.streamValues
  def put(ov: FlinkClusterOverview): IO[FlinkDataStoreErr, Unit]           = stg.put(ov.fcid, ov)
  def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]                          = stg.delete(fcid)

class JmMetricsMemoryStorage(ref: Ref[mutable.Map[Fcid, FlinkJmMetrics]]) extends JmMetricsStorage:
  private val stg                                                 = MapBasedStg(ref)
  def get(fcid: Fcid): IO[FlinkDataStoreErr, Option[FlinkJmMetrics]] = stg.get(fcid)
  def put(metric: FlinkJmMetrics): IO[FlinkDataStoreErr, Unit]       = stg.put(Fcid(metric.clusterId, metric.namespace), metric)
  def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]                    = stg.delete(fcid)

class TmDetailMemoryStorage(ref: Ref[mutable.Map[Ftid, FlinkTmDetail]]) extends TmDetailStorage:
  private val stg                                                = MapBasedStg(ref)
  def put(tm: FlinkTmDetail): IO[FlinkDataStoreErr, Unit]           = stg.put(tm.ftid, tm)
  def putAll(tm: List[FlinkTmDetail]): IO[FlinkDataStoreErr, Unit]  = stg.putAll(tm.map(tm => tm.ftid -> tm).toMap)
  def rm(ftid: Ftid): IO[FlinkDataStoreErr, Unit]                   = stg.delete(ftid)
  def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]                   = stg.deleteByKey(_.fcid == fcid)
  def get(ftid: Ftid): IO[FlinkDataStoreErr, Option[FlinkTmDetail]] = stg.get(ftid)
  def list(fcid: Fcid): IO[FlinkDataStoreErr, List[FlinkTmDetail]]  = stg.getByKey(_.fcid == fcid)
  def listAll: Stream[FlinkDataStoreErr, FlinkTmDetail]             = stg.streamValues
  def listTmId(fcid: Fcid): IO[FlinkDataStoreErr, List[Ftid]]       = stg.getPartByKey(_.fcid == fcid, _.ftid)
  def listAllTmId: Stream[FlinkDataStoreErr, Ftid]                  = stg.streamValues.map(_.ftid)

class TmMetricMemoryStorage(ref: Ref[mutable.Map[Ftid, FlinkTmMetrics]]) extends TmMetricStorage:
  private val stg                                                 = MapBasedStg(ref)
  def put(metric: FlinkTmMetrics): IO[FlinkDataStoreErr, Unit]       = stg.put(Ftid(metric.clusterId, metric.namespace, metric.tmId), metric)
  def rm(ftid: Ftid): IO[FlinkDataStoreErr, Unit]                    = stg.delete(ftid)
  def rm(fcid: Fcid): IO[FlinkDataStoreErr, Unit]                    = stg.deleteByKey(_.fcid == fcid)
  def get(ftid: Ftid): IO[FlinkDataStoreErr, Option[FlinkTmMetrics]] = stg.get(ftid)
  def list(fcid: Fcid): IO[FlinkDataStoreErr, List[FlinkTmMetrics]]  = stg.getByKey(_.fcid == fcid)
  def listTmId(fcid: Fcid): IO[FlinkDataStoreErr, List[Ftid]]        = stg.getKeys.map(_.filter(_.fcid == fcid))
