package potamoi.fs.refactor

import potamoi.fs.refactor.FsErr.UnSupportedSchema
import potamoi.zios.asLayer
import potamoi.syntax.contra
import zio.*
import zio.http.*
import zio.http.model.{Method, Status}
import zio.http.Http.status
import zio.stream.ZStream

/**
 * Potamoi remote http file server application.
 * The actual storage backend depends on [[RemoteFsOperator]].
 */
object FileServer:

  /**
   * Running filer server app.
   */
  def run: ZIO[RemoteFsOperator with FileServerConf, Throwable, Unit] =
    for {
      conf    <- ZIO.service[FileServerConf]
      backend <- ZIO.service[RemoteFsOperator]
      app                 = FileServer(conf, backend)
      httpServerConfLayer = ServerConfig.live.project(_.binding(conf.host, conf.port))

      _ <- ZIO.logInfo(s"""Potamoi file remote server start at http://${conf.host}:${conf.port}
                          |API List:
                          |- GET /heath
                          |- GET /fs/<file-path>""".stripMargin)
      _ <- Server
        .serve(app.routes)
        .provideLayer(ServerConfig.live ++ httpServerConfLayer >>> Server.live ++ Scope.default)
    } yield ()

  /**
   * Get the remote http access address corresponding to the path in pota schema format.
   * For example: "pota://aa/bb.txt" to "http://127.0.0.1:3400/fs/aa/bb.txt"
   */
  def getRemoteHttpFilePath(path: String, conf: FileServerConf): IO[UnSupportedSchema, String] = {
    paths.getSchema(path) match
      case None                                         => ZIO.fail(UnSupportedSchema(path))
      case Some(schema) if schema != paths.potaFsSchema => ZIO.fail(UnSupportedSchema(path))
      case Some(_) =>
        val purePath = paths.rmSchema andThen paths.rmFirstSlash apply path
        ZIO.succeed(s"http://${conf.host}:${conf.port}/fs/$purePath")
  }

class FileServer(conf: FileServerConf, backend: RemoteFsOperator):

  val routes = Http.collectHttp[Request] {
    case Method.GET -> !! / "health" => Http.ok
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
