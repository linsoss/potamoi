package potamoi.fs

import FsErr.UnSupportedSchema
import potamoi.syntax.contra
import potamoi.zios.asLayer
import potamoi.BaseConf
import zio.*
import zio.http.*
import zio.http.model.{Method, Status}
import zio.http.Http.status
import zio.stream.ZStream

import java.net.InetSocketAddress

/**
 * Potamoi remote http file server application.
 * The actual storage backend depends on [[RemoteFsOperator]].
 */
object FileServer:

  /**
   * Running filer server app.
   */
  def run: ZIO[RemoteFsOperator with FileServerConf with BaseConf, Throwable, Unit] =
    for {
      conf     <- ZIO.service[FileServerConf]
      baseConf <- ZIO.service[BaseConf]
      backend  <- ZIO.service[RemoteFsOperator]
      app       = FileServer(backend)
      _        <- ZIO.logInfo(s"""Potamoi file remote server start at http://${baseConf.svcDns}:${conf.port}
                          |API List:
                          |- GET /heath
                          |- GET /fs/<file-path>""".stripMargin)
      _        <- Server
                    .serve(app.routes)
                    .provide(
                      ServerConfig.live ++ ServerConfig.live.project(_.binding(InetSocketAddress(conf.port))),
                      Server.live,
                      Scope.default
                    )
    } yield ()

  /**
   * Get the remote http access address corresponding to the path in pota schema format.
   * For example: "pota://aa/bb.txt" to "http://127.0.0.1:3400/fs/aa/bb.txt"
   */
  def getRemoteHttpFilePath(path: String): ZIO[FileServerConf with BaseConf, UnSupportedSchema, String] = {
    paths.getSchema(path) match
      case None                                         => ZIO.fail(UnSupportedSchema(path))
      case Some(schema) if schema != paths.potaFsSchema => ZIO.fail(UnSupportedSchema(path))
      case Some(_)                                      =>
        for
          host    <- ZIO.service[BaseConf].map(_.svcDns)
          port    <- ZIO.service[FileServerConf].map(_.port)
          purePath = paths.rmSchema andThen paths.rmFirstSlash apply path
          httpPath = s"http://${host}:${port}/fs/$purePath"
        yield httpPath
  }

class FileServer(backend: RemoteFsOperator):

  val routes = Http.collectHttp[Request] {
    case Method.GET -> !! / "health"      => Http.ok
    case Method.GET -> "" /: "fs" /: path =>
      val pathStr = path.toString
      Http
        .collectZIO[Request] { _ =>
          backend.exist(pathStr).map {
            case false => Http.notFound
            case true  => Http.fromStream(backend.downloadAsStream(pathStr))
          }
        }
        .flatten
  }
