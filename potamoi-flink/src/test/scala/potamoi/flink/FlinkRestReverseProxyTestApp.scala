package potamoi.flink

import potamoi.{BaseConf, HoconConfig, NodeRoles}
import potamoi.akka.{AkkaConf, AkkaMatrix}
import potamoi.flink.observer.FlinkObserver
import potamoi.flink.operator.FlinkOperator
import potamoi.flink.storage.FlinkDataStorage
import potamoi.fs.{FileServerConf, RemoteFsOperator, S3FsBackendConf}
import potamoi.kubernetes.{K8sConf, K8sOperator}
import potamoi.logger.{LogConf, PotaLogger}
import potamoi.syntax.*
import potamoi.zios.*
import potamoi.BaseConfDev.given
import potamoi.FsBackendConfDev.given
import zio.*
import zio.http.{Client, Server}

object FlinkRestReverseProxyTest extends ZIOAppDefault:

  override val bootstrap = PotaLogger.default

  val program = {
    for {
      opr <- ZIO.service[FlinkOperator]
      obr <- ZIO.service[FlinkObserver]
      _   <- obr.manager.track("app-t1" -> "fdev")
      _   <- obr.manager.track("app-t2" -> "fdev")
      _   <- opr.restProxy.enable("app-t1" -> "fdev")
      _   <- opr.restProxy.enable("app-t2" -> "fdev")
    } yield ()
  } *> Server.serve(FlinkRestReverseProxy.route)

  val run = program
    .provide(
      HoconConfig.empty,
      LogConf.default,
      FileServerConf.default,
      BaseConf.test,
      S3FsBackendConf.test,
      RemoteFsOperator.live,
      FlinkConf.test,
      K8sConf.default,
      K8sOperator.live,
      FlinkDataStorage.test,
      FlinkObserver.live,
      FlinkOperator.live,
      FlinkRestProxyProvider.live,
      AkkaConf.local(List(NodeRoles.flinkService)),
      AkkaMatrix.live,
      Server.default,
      Client.default,
      Scope.default
    )
