package potamoi.flink.interpreter

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.receptionist.ServiceKey
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.sharding.typed.ClusterShardingSettings.PassivationStrategySettings
import potamoi.akka.{behaviors, AkkaMatrix, ShardingProxy}
import potamoi.flink.{FlinkConf, FlinkMajorVer}
import potamoi.logger.LogConf
import potamoi.NodeRoles
import potamoi.fs.RemoteFsOperator
import potamoi.times.given_Conversion_ScalaDuration_FiniteDuration
import zio.{IO, Task, ZIO}

/**
 * Flink sql interpreter management manager.
 */
object FlinkInterpreterPier {

  /**
   * Activate the corresponding version of the flink interactor management actor.
   */
  def active(flinkVer: FlinkMajorVer): ZIO[RemoteFsOperator with FlinkConf with LogConf with AkkaMatrix, Throwable, ActorRef[FlinkInterpreter.Req]] =
    for {
      actorCradle <- ZIO.service[AkkaMatrix]
      logConf     <- ZIO.service[LogConf]
      flinkConf   <- ZIO.service[FlinkConf]
      remoteFs    <- ZIO.service[RemoteFsOperator]
      interpreter <- actorCradle.spawn(s"flink-interpreter-proxy-${flinkVer.seq}", FlinkInterpreter(flinkVer, logConf, flinkConf, remoteFs))
    } yield interpreter

  /**
   * Activate all version of the flink interactor management actor on [[FlinkMajorVer.values]].
   */
  def activeAll: ZIO[RemoteFsOperator with FlinkConf with LogConf with AkkaMatrix, Throwable, Map[FlinkMajorVer, ActorRef[FlinkInterpreter.Req]]] =
    ZIO
      .foreach(FlinkMajorVer.values)(ver => active(ver).map(actor => ver -> actor))
      .map(_.toMap)
}

object FlinkInterpreter extends ShardingProxy[String, FlinkInterpreterActor.Cmd] {

  private var logConfInstance: Option[LogConf]           = None
  private var remoteFsInstance: Option[RemoteFsOperator] = None

  private[interpreter] def unsafeLogConf: Option[LogConf]           = logConfInstance
  private[interpreter] def unsafeRemoteFs: Option[RemoteFsOperator] = remoteFsInstance

  val ServiceKeys: Map[FlinkMajorVer, ServiceKey[FlinkInterpreter.Req]] =
    FlinkMajorVer.values.map(ver => ver -> ServiceKey[Req](s"flink-interpreter-svc-$ver")).toMap

  def apply(
      flinkVer: FlinkMajorVer,
      logConf: LogConf,
      flinkConf: FlinkConf,
      remoteFs: RemoteFsOperator): Behavior[Req] = {
    logConfInstance = Some(logConf)
    remoteFsInstance = Some(remoteFs)
    behavior(
      entityKey = EntityTypeKey[FlinkInterpreterActor.Cmd](s"flink-interpreter-${flinkVer.seq}"),
      marshallKey = identity,
      unmarshallKey = identity,
      createBehavior = sessionId => FlinkInterpreterActor(sessionId),
      stopMessage = Some(FlinkInterpreterActor.Terminate),
      bindRole = Some(flinkVer.nodeRole),
      passivation = Some(PassivationStrategySettings.defaults.withIdleEntityPassivation(flinkConf.sqlInteract.maxIdleTimeout)),
      serviceKeyRegister = Some(ServiceKeys(flinkVer)) -> Some(flinkVer.nodeRole)
    )
  }

}
