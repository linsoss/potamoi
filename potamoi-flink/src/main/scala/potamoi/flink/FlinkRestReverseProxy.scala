package potamoi.flink

import potamoi.common.Err
import potamoi.flink.model.{Fcid, FlinkRestSvcEndpoint}
import potamoi.flink.storage.FlinkSnapshotStorage
import potamoi.times.given_Conversion_ScalaDuration_ZioDuration
import zio.http.*
import zio.http.model.Status
import zio.ZIO.{logDebug, logInfo}
import zio.cache.{Cache, Lookup}
import zio.{durationInt, IO, ZIO, ZLayer}

/**
 * Flink http rest reverse proxy routes.
 */
object FlinkRestReverseProxy:
  val route = Http.collectZIO[Request] { case req @ _ -> "" /: "proxy" /: "flink" /: namespace /: clusterId /: path =>
    FlinkRestProxyProvider.proxy(Fcid(clusterId, namespace), path, req)
  }

/**
 * Proxy provider for single flink cluster.
 */
trait FlinkRestProxyProvider:
  def proxy(fcid: Fcid, path: Path, req: Request): IO[Throwable, Response]

object FlinkRestProxyProvider {

  def proxy(fcid: Fcid, path: Path, req: Request): ZIO[FlinkRestProxyProvider, Throwable, Response] =
    ZIO.serviceWithZIO[FlinkRestProxyProvider](_.proxy(fcid, path, req))

  val live = ZLayer {
    for {
      flinkConf   <- ZIO.service[FlinkConf]
      snapStorage <- ZIO.service[FlinkSnapshotStorage]
      client      <- ZIO.service[Client]
      eptRouteTable <- Cache.make[Fcid, Any, Throwable, FlinkRestSvcEndpoint](
        capacity = flinkConf.reverseProxy.routeTableCacheSize,
        timeToLive = flinkConf.reverseProxy.routeTableCacheTtl,
        lookup = Lookup(fcid =>
          for {
            _   <- snapStorage.restProxy.exists(fcid).flatMap(ZIO.fail(EndpointNotFound).unless(_))
            ept <- snapStorage.restEndpoint.get(fcid).someOrFail(EndpointNotFound)
          } yield ept)
      )
    } yield Live(flinkConf, snapStorage, client, eptRouteTable)
  }

  private case object EndpointNotFound extends Err()

  case class Live(
      flinkConf: FlinkConf,
      snapStg: FlinkSnapshotStorage,
      zClient: Client,
      eptRouteTable: Cache[Fcid, Throwable, FlinkRestSvcEndpoint])
      extends FlinkRestProxyProvider {
    private given FlinkRestEndpointType = flinkConf.restEndpointTypeInternal

    override def proxy(fcid: Fcid, path: Path, req: Request): IO[Throwable, Response] = {
      for {
        ept        <- eptRouteTable.get(fcid)
        forwardUrl <- ZIO.succeed(req.url.setHost(ept.chooseHost).setPort(ept.port).setPath(path))
        _          <- logDebug(s"Proxy flink uri: ${forwardUrl.toJavaURI.toString}")
        rsp        <- zClient.request(req.copy(url = forwardUrl))
      } yield rsp
    }.catchSome { case EndpointNotFound => ZIO.succeed(Response.status(Status.NotFound)) }
  }

}
