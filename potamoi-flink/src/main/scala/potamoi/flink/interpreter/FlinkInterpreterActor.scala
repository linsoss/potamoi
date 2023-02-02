package potamoi.flink.interpreter

import akka.actor.typed.{ActorRef, Behavior, PostStop}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import potamoi.akka.zios.*
import potamoi.common.Ack
import potamoi.flink.{FlinkConf, FlinkInteractErr}
import potamoi.flink.model.interact.*
import potamoi.flink.FlinkInteractErr.*
import potamoi.flink.FlinkInterpreterErr.{HandleNotFound, ResultNotFound, SplitSqlScriptErr}
import potamoi.fs.refactor.RemoteFsOperator
import potamoi.logger.LogConf
import potamoi.options.unsafeGet
import potamoi.KryoSerializable
import zio.{CancelableFuture, Schedule, Scope, ZIO}
import zio.ZIO.{fail, succeed}

/**
 * Flink sql interpretive worker actor.
 */
object FlinkInterpreterActor {

  type Reply[E, A]             = ActorRef[Either[E, A]]
  type InternalSubmitScriptErr = (SessionNotYetStarted | FailToSplitSqlScript) with FlinkInteractErr
  type InternalAttachHandleErr = (SessionNotYetStarted | SessionHandleNotFound) with FlinkInteractErr

  sealed trait Cmd extends KryoSerializable

  final case class Start(sessionDef: SessionDef, updateConflict: Boolean = false, reply: ActorRef[Ack.type]) extends Cmd
  final case class Cancel(reply: ActorRef[Ack.type])                                                         extends Cmd
  final case class Stop(reply: ActorRef[Ack.type])                                                           extends Cmd

  case object Terminate extends Cmd

  final case class CompleteSql(sql: String, position: Int, reply: Reply[SessionNotYetStarted, List[String]])          extends Cmd
  final case class SubmitSqlAsync(sql: String, handleId: String, reply: Reply[SessionNotYetStarted, Ack.type])        extends Cmd
  final case class SubmitSqlScriptAsync(sqlScript: String, reply: Reply[InternalSubmitScriptErr, List[ScripSqlSign]]) extends Cmd

  final case class RetrieveResultPage(
      handleId: String,
      page: Int,
      pageSize: Int,
      reply: Reply[InternalAttachHandleErr, Option[SqlResultPageView]])
      extends Cmd

  final case class RetrieveResultOffset(
      handleId: String,
      offset: Long,
      chunkSize: Int,
      reply: Reply[InternalAttachHandleErr, Option[SqlResultOffsetView]])
      extends Cmd

  final case class Overview(reply: ActorRef[SessionOverview])                                                 extends Cmd
  final case class ListHandleId(reply: Reply[SessionNotYetStarted, List[String]])                             extends Cmd
  final case class ListHandleStatus(reply: Reply[SessionNotYetStarted, List[HandleStatusView]])               extends Cmd
  final case class ListHandleFrame(reply: Reply[SessionNotYetStarted, List[HandleFrame]])                     extends Cmd
  final case class GetHandleStatus(handleId: String, reply: Reply[InternalAttachHandleErr, HandleStatusView]) extends Cmd
  final case class GetHandleFrame(handleId: String, reply: Reply[InternalAttachHandleErr, HandleFrame])       extends Cmd

  /**
   * Actor behavior.
   */
  def apply(sessionId: String): Behavior[Cmd] = Behaviors.setup { ctx =>
    ctx.log.info(s"Flink sql interpreter start: sessionId=$sessionId")
    new FlinkInterpreterActor(sessionId)(using ctx).active
  }
}

import potamoi.flink.interpreter.FlinkInterpreterActor.*

class FlinkInterpreterActor(sessionId: String)(using ctx: ActorContext[Cmd]) {

  private given LogConf = FlinkInterpreter.unsafeLogConf.unsafeGet
  private val remoteFs  = FlinkInterpreter.unsafeRemoteFs.unsafeGet

  // actor state
  private var curSessDef: Option[SessionDef]         = None
  private var sqlExecutor: Option[SerialSqlExecutor] = None

  /**
   * Actor active behavior.
   */
  def active: Behavior[Cmd] = Behaviors
    .receiveMessage[Cmd] {

      case Start(sessDef, updateConflict, reply) =>
        // create and run executor
        if (sqlExecutor.isEmpty) {
          launchSqlExecutor(sessDef)
        }
        // reset executor and relaunch it
        else if (!curSessDef.contains(sessDef) && updateConflict) {
          sqlExecutor.foreach(_.stop.runPureSync)
          launchSqlExecutor(sessDef)
        }
        reply ! Ack
        Behaviors.same

      case Stop(reply) =>
        sqlExecutor.foreach(_.stop.runPureSync)
        sqlExecutor = None
        reply ! Ack
        Behaviors.stopped

      case Cancel(reply) =>
        sqlExecutor.foreach(_.cancel.runPureSync)
        reply ! Ack
        Behaviors.same

      case Terminate =>
        sqlExecutor.foreach(_.stop.runPureSync)
        sqlExecutor = None
        Behaviors.stopped

      case Overview(reply) =>
        val (isStarted, isBusy) = sqlExecutor match {
          case None           => false -> false
          case Some(executor) => (executor.isStarted <&> executor.isBusy).runPureSync
        }
        reply ! SessionOverview(sessionId, isStarted, isBusy, curSessDef)
        Behaviors.same

      case ListHandleId(reply) =>
        sqlExecutor match
          case None           => reply ! Left(SessionNotYetStarted(sessionId))
          case Some(executor) => reply ! Right(executor.listHandleId.runPureSync)
        Behaviors.same

      case ListHandleStatus(reply) =>
        sqlExecutor match
          case None           => reply ! Left(SessionNotYetStarted(sessionId))
          case Some(executor) => reply ! Right(executor.listHandleStatus.runPureSync)
        Behaviors.same

      case ListHandleFrame(reply) =>
        sqlExecutor match
          case None           => reply ! Left(SessionNotYetStarted(sessionId))
          case Some(executor) => reply ! Right(executor.listHandleFrame.runPureSync)
        Behaviors.same

      case GetHandleStatus(handleId, reply) =>
        val rs = sqlExecutor match
          case None           => Left(SessionNotYetStarted(sessionId))
          case Some(executor) => executor.getHandleStatus(handleId).mapError(e => SessionHandleNotFound(sessionId, e.handleId)).runSync
        reply ! rs
        Behaviors.same

      case GetHandleFrame(handleId, reply) =>
        val rs = sqlExecutor match
          case None           => Left(SessionNotYetStarted(sessionId))
          case Some(executor) => executor.getHandleFrame(handleId).mapError(e => SessionHandleNotFound(sessionId, e.handleId)).runSync
        reply ! rs
        Behaviors.same

      case CompleteSql(sql, position, reply) =>
        val rs = sqlExecutor match
          case None           => Left(SessionNotYetStarted(sessionId))
          case Some(executor) => executor.completeSql(sql, position).map(Right(_)).runPureSync
        reply ! rs
        Behaviors.same

      case SubmitSqlAsync(sql, handleId, reply) =>
        val rs = sqlExecutor match
          case None           => Left(SessionNotYetStarted(sessionId))
          case Some(executor) => executor.submitSqlAsync(sql, handleId).as(Ack).runSync
        reply ! rs
        Behaviors.same

      case SubmitSqlScriptAsync(sqlScript, reply) =>
        val rs = sqlExecutor match
          case None           => Left(SessionNotYetStarted(sessionId))
          case Some(executor) =>
            executor
              .submitSqlScriptAsync(sqlScript)
              .foldCauseZIO(cause => fail(FailToSplitSqlScript(cause.failures.head, cause.prettyPrint)), rs => succeed(rs.map(_._1)))
              .runSync
        reply ! rs
        Behaviors.same

      case RetrieveResultPage(handleId, page, pageSize, reply) =>
        val rs = sqlExecutor match
          case None           => Left(SessionNotYetStarted(sessionId))
          case Some(executor) =>
            executor
              .retrieveResultPage(handleId, page, pageSize)
              .map(Some(_))
              .catchSome { case ResultNotFound(_) => succeed(None) }
              .orElseFail(SessionHandleNotFound(sessionId, handleId))
              .runSync
        reply ! rs
        Behaviors.same

      case RetrieveResultOffset(handleId, offset, chunkSize, reply) =>
        val rs = sqlExecutor match
          case None           => Left(SessionNotYetStarted(sessionId))
          case Some(executor) =>
            executor
              .retrieveResultOffset(handleId, offset, chunkSize)
              .map(Some(_))
              .catchSome { case ResultNotFound(_) => succeed(None) }
              .orElseFail(SessionHandleNotFound(sessionId, handleId))
              .runSync
        reply ! rs
        Behaviors.same

    }
    .receiveSignal { case (_, PostStop) =>
      // ensure close executor fiber
      sqlExecutor.foreach(_.stop.runPureSync)
      ctx.log.info(s"Flink sql interpreter stopped: sessionId=$sessionId")
      Behaviors.same
    }

  private def launchSqlExecutor(sessDef: SessionDef): Unit = {
    val executor = SerialSqlExecutor.create(sessionId, sessDef, remoteFs).runPureSync
    executor.start.provideLayer(Scope.default).runSync
    sqlExecutor = Some(executor)
    curSessDef = Some(sessDef)
  }

}
