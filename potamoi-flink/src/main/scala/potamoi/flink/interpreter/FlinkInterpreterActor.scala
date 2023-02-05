package potamoi.flink.interpreter

import akka.actor.typed.{ActorRef, Behavior, PostStop}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import potamoi.akka.zios.*
import potamoi.flink.{FlinkConf, FlinkInteractErr}
import potamoi.flink.model.interact.*
import potamoi.flink.FlinkInteractErr.*
import potamoi.flink.FlinkInterpreterErr.{HandleNotFound, ResultNotFound, SplitSqlScriptErr}
import potamoi.fs.refactor.RemoteFsOperator
import potamoi.logger.LogConf
import potamoi.options.unsafeGet
import potamoi.KryoSerializable
import potamoi.akka.*
import potamoi.flink.model.{FlinkRuntimeMode, FlinkTargetType}
import zio.{CancelableFuture, Schedule, Scope, ZIO}
import zio.ZIO.{fail, succeed}

/**
 * Flink sql interpretive worker actor.
 */
object FlinkInterpreterActor {

  type InternalSubmitScriptErr = (SessionNotYetStarted | FailToSplitSqlScript) with FlinkInteractErr
  type InternalAttachHandleErr = (SessionNotYetStarted | SessionHandleNotFound) with FlinkInteractErr

  sealed trait Cmd extends KryoSerializable

  final case class Start(sessionDef: SessionSpec, updateConflict: Boolean = false, reply: ValueReply[Ack.type]) extends Cmd
  final case class Cancel(reply: ValueReply[Ack.type])                                                         extends Cmd
  final case class Stop(reply: ValueReply[Ack.type])                                                           extends Cmd

  case object Terminate extends Cmd

  final case class CompleteSql(sql: String, position: Int, reply: EitherReply[SessionNotYetStarted, List[String]])          extends Cmd
  final case class SubmitSqlAsync(sql: String, handleId: String, reply: EitherReply[SessionNotYetStarted, Ack.type])        extends Cmd
  final case class SubmitSqlScriptAsync(sqlScript: String, reply: EitherReply[InternalSubmitScriptErr, List[ScripSqlSign]]) extends Cmd

  final case class RetrieveResultPage(
      handleId: String,
      page: Int,
      pageSize: Int,
      reply: EitherReply[InternalAttachHandleErr, Option[SqlResultPageView]])
      extends Cmd

  final case class RetrieveResultOffset(
      handleId: String,
      offset: Long,
      chunkSize: Int,
      reply: EitherReply[InternalAttachHandleErr, Option[SqlResultOffsetView]])
      extends Cmd

  final case class Overview(reply: ValueReply[SessionOverview])                                                     extends Cmd
  final case class ListHandleId(reply: EitherReply[SessionNotYetStarted, List[String]])                             extends Cmd
  final case class ListHandleStatus(reply: EitherReply[SessionNotYetStarted, List[HandleStatusView]])               extends Cmd
  final case class ListHandleFrame(reply: EitherReply[SessionNotYetStarted, List[HandleFrame]])                     extends Cmd
  final case class GetHandleStatus(handleId: String, reply: EitherReply[InternalAttachHandleErr, HandleStatusView]) extends Cmd
  final case class GetHandleFrame(handleId: String, reply: EitherReply[InternalAttachHandleErr, HandleFrame])       extends Cmd

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
  private var curSessDef: Option[SessionSpec]         = None
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
        reply ! pack(Ack)
        Behaviors.same

      case Stop(reply) =>
        sqlExecutor.foreach(_.stop.runPureSync)
        sqlExecutor = None
        reply ! pack(Ack)
        Behaviors.stopped

      case Cancel(reply) =>
        sqlExecutor.foreach(_.cancel.runPureSync)
        reply ! pack(Ack)
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
        reply ! pack(SessionOverview(sessionId, isStarted, isBusy, curSessDef))
        Behaviors.same

      case ListHandleId(reply) =>
        val rs = sqlExecutor match
          case None           => Left(SessionNotYetStarted(sessionId))
          case Some(executor) => Right(executor.listHandleId.runPureSync)
        reply ! packEither(rs)
        Behaviors.same

      case ListHandleStatus(reply) =>
        val rs = sqlExecutor match
          case None           => Left(SessionNotYetStarted(sessionId))
          case Some(executor) => Right(executor.listHandleStatus.runPureSync)
        reply ! packEither(rs)
        Behaviors.same

      case ListHandleFrame(reply) =>
        val rs = sqlExecutor match
          case None           => Left(SessionNotYetStarted(sessionId))
          case Some(executor) => Right(executor.listHandleFrame.runPureSync)
        reply ! packEither(rs)
        Behaviors.same

      case GetHandleStatus(handleId, reply) =>
        val rs = sqlExecutor match
          case None           => Left(SessionNotYetStarted(sessionId))
          case Some(executor) => executor.getHandleStatus(handleId).mapError(e => SessionHandleNotFound(sessionId, e.handleId)).runSync
        reply ! packEither(rs)
        Behaviors.same

      case GetHandleFrame(handleId, reply) =>
        val rs = sqlExecutor match
          case None           => Left(SessionNotYetStarted(sessionId))
          case Some(executor) => executor.getHandleFrame(handleId).mapError(e => SessionHandleNotFound(sessionId, e.handleId)).runSync
        reply ! packEither(rs)
        Behaviors.same

      case CompleteSql(sql, position, reply) =>
        val rs = sqlExecutor match
          case None           => Left(SessionNotYetStarted(sessionId))
          case Some(executor) => executor.completeSql(sql, position).map(Right(_)).runPureSync
        reply ! packEither(rs)
        Behaviors.same

      case SubmitSqlAsync(sql, handleId, reply) =>
        val rs = sqlExecutor match
          case None           => Left(SessionNotYetStarted(sessionId))
          case Some(executor) => executor.submitSqlAsync(sql, handleId).as(Ack).runSync
        reply ! packEither(rs)
        Behaviors.same

      case SubmitSqlScriptAsync(sqlScript, reply) =>
        val rs = sqlExecutor match
          case None           => Left(SessionNotYetStarted(sessionId))
          case Some(executor) =>
            executor
              .submitSqlScriptAsync(sqlScript)
              .foldCauseZIO(cause => fail(FailToSplitSqlScript(cause.failures.head, cause.prettyPrint)), rs => succeed(rs))
              .runSync
        reply ! packEither(rs)
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
        reply ! packEither(rs)
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
        reply ! packEither(rs)
        Behaviors.same

    }
    .receiveSignal { case (_, PostStop) =>
      // ensure close executor fiber
      sqlExecutor.foreach(_.stop.runPureSync)
      ctx.log.info(s"Flink sql interpreter stopped: sessionId=$sessionId")
      Behaviors.same
    }

  private def launchSqlExecutor(sessDef: SessionSpec): Unit = {
    val executor = SerialSqlExecutor.create(sessionId, sessDef, remoteFs).runPureSync
    // question
    sqlExecutor = Some(executor)
    (executor.start *> ZIO.never).provideLayer(Scope.default).runAsync
    curSessDef = Some(sessDef)
  }

}
