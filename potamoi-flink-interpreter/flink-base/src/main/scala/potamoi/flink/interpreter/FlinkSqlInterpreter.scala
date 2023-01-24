package potamoi.flink.interpreter

import com.devsisters.shardcake.{EntityType, Replier, Sharding}
import potamoi.common.Ack
import potamoi.flink.{FlinkInteractErr, FlinkInterpreterErr}
import potamoi.flink.model.interact.*
import potamoi.flink.protocol.{FlinkInterpEntity, FlinkInterpProto, InternalRpcProto}
import potamoi.flink.FlinkInteractErr.{FailToSplitSqlScript, SessionHandleNotFound, SessionNotYetStarted}
import potamoi.flink.model.interact.SqlResult.toView
import potamoi.flink.FlinkInterpreterErr.{HandleNotFound, ResultNotFound}
import potamoi.fs.refactor.RemoteFsOperator
import potamoi.rpc.RpcClient
import potamoi.sharding.ShardRegister
import potamoi.zios.someOrUnit
import zio.{Dequeue, Duration, IO, Ref, RIO, Scope, UIO, URIO, ZIO, ZIOAspect, ZLayer}
import zio.ZIO.{executor, fail, succeed}
import zio.ZIOAspect.annotated

import scala.reflect.ClassTag

/**
 * Flink sql interactor sharding entity.
 * see:
 * [[potamoi.flink.protocol.FlinkInterpEntity]]
 * [[potamoi.flink.protocol.FlinkInterpProto]]
 */
object FlinkSqlInterpreter:

  def live(entity: EntityType[FlinkInterpProto]): ZLayer[RemoteFsOperator, Nothing, FlinkSqlInterpreter] =
    ZLayer {
      for {
        remoteFs <- ZIO.service[RemoteFsOperator]
      } yield FlinkSqlInterpreter(entity, remoteFs)
    }

  private case class State(sessionDef: Ref[Option[SessionDef]], executor: Ref[Option[SerialSqlExecutor]])

  private object State:
    def make: UIO[State] = for {
      sessDef  <- Ref.make[Option[SessionDef]](None)
      executor <- Ref.make[Option[SerialSqlExecutor]](None)
    } yield State(sessDef, executor)

/**
 * Default implementation
 */
class FlinkSqlInterpreter(entity: EntityType[FlinkInterpProto], remoteFs: RemoteFsOperator) extends ShardRegister:

  import FlinkInterpProto.*
  import FlinkSqlInterpreter.*

  override private[potamoi] def registerEntities: URIO[Sharding with Scope, Unit] = {
    Sharding.registerEntity(entity, behavior)
  }

  /**
   * Sharding behaviors.
   */
  def behavior(sessionId: String, messages: Dequeue[FlinkInterpProto]): RIO[Sharding with Scope, Nothing] =
    for {
      state    <- State.make
      handlers <- messages.take.flatMap(handleMessage(_)(using state, sessionId)).forever @@ annotated("sessionId" -> sessionId)
    } yield handlers

  private def handleMessage(message: FlinkInterpProto)(using state: State, sessionId: String): RIO[Sharding with Scope, Unit] =
    message match {

      case Start(sessDef, updateConflict, replier) =>
        handleStart(sessDef, updateConflict) *>
        replier.reply(Ack)

      case CancelCurrentHandles(replier) =>
        state.executor.get.someOrUnit(_.cancel) *>
        replier.reply(Ack)

      case Stop(replier) =>
        state.executor.get.someOrUnit(_.stop) *>
        state.executor.set(None) *>
        state.sessionDef.set(None) *>
        replier.reply(Ack)

      case Terminate =>
        state.executor.get.someOrUnit(_.stop) *>
        state.executor.set(None) *>
        state.sessionDef.set(None)

      case CompleteSql(sql, position, replier) =>
        getExecutor
          .flatMap { executor => executor.completeSql(sql, position) }
          .either
          .flatMap(replier.reply)
          .async

      case SubmitSqlAsync(sql, handleId, replier) =>
        getExecutor
          .flatMap { executor => executor.submitSqlAsync(sql, handleId).as(Ack) }
          .either
          .flatMap(replier.reply)
          .async

      case SubmitSqlScriptAsync(sqlScript, replier) =>
        getExecutor
          .flatMap { executor =>
            executor
              .submitSqlScriptAsync(sqlScript)
              .foldCauseZIO(cause => fail(FailToSplitSqlScript(cause.failures.head, cause.prettyPrint)), rs => succeed(rs.map(_._1)))
          }
          .either
          .flatMap { case r: Either[SessionNotYetStarted | FailToSplitSqlScript, List[ScripSqlSign]] => replier.reply(r) }
          .async

      case RetrieveResultPage(handleId, page, pageSize, replier) =>
        getExecutor
          .flatMap { executor =>
            executor
              .retrieveResultPage(handleId, page, pageSize)
              .map(Some(_))
              .catchSome { case ResultNotFound(_) => succeed(None) }
              .orElseFail(SessionHandleNotFound(sessionId, handleId))
          }
          .either
          .flatMap { case r: Either[SessionNotYetStarted | SessionHandleNotFound, Option[SqlResultPageView]] => replier.reply(r) }
          .async

      case RetrieveResultOffset(handleId, offset, chunkSize, replier) =>
        getExecutor
          .flatMap { executor =>
            executor
              .retrieveResultOffset(handleId, offset, chunkSize)
              .map(Some(_))
              .catchSome { case ResultNotFound(_) => succeed(None) }
              .orElseFail(SessionHandleNotFound(sessionId, handleId))
          }
          .either
          .flatMap { case r: Either[SessionNotYetStarted | SessionHandleNotFound, Option[SqlResultOffsetView]] => replier.reply(r) }
          .async

      case ListHandleId(replier) =>
        getExecutor
          .flatMap { executor => executor.listHandleId }
          .either
          .flatMap(replier.reply)
          .async

      case ListHandleStatus(replier) =>
        getExecutor
          .flatMap { executor => executor.listHandleStatus }
          .either
          .flatMap(replier.reply)
          .async

      case ListHandleFrame(replier) =>
        getExecutor
          .flatMap { executor => executor.listHandleFrame }
          .either
          .flatMap(replier.reply)
          .async

      case GetHandleStatus(handleId, replier) =>
        getExecutor
          .flatMap { executor =>
            executor
              .getHandleStatus(handleId)
              .mapBoth(_ => SessionHandleNotFound(sessionId, handleId), e => Some(e))
          }
          .either
          .flatMap { case r: Either[SessionNotYetStarted | SessionHandleNotFound, HandleStatusView] => replier.reply(r) }
          .async

      case GetHandleFrame(handleId, replier) =>
        getExecutor
          .flatMap { executor =>
            executor
              .getHandleFrame(handleId)
              .mapBoth(_ => SessionHandleNotFound(sessionId, handleId), e => Some(e))
          }
          .either
          .flatMap { case r: Either[SessionNotYetStarted | SessionHandleNotFound, HandleFrame] => replier.reply(r) }
          .async
    }

  /**
   * Launch or Relaunch flink serial sql executor.
   */
  private def handleStart(sessDef: SessionDef, updateConflict: Boolean)(using state: State, sessionId: String): URIO[Scope, Unit] =
    (state.executor.get <&> state.sessionDef.get).flatMap {
      // create and run executor
      case (None, _)                                                                     =>
        for {
          executor <- SerialSqlExecutor.create(sessionId, sessDef, remoteFs)
          _        <- state.sessionDef.set(Some(sessDef))
          _        <- state.executor.set(Some(executor))
          _        <- executor.start
        } yield ()
      // reset executor and relaunch it
      case (Some(executor), Some(oldSessDef)) if oldSessDef != sessDef && updateConflict =>
        for {
          _           <- executor.stop
          newExecutor <- SerialSqlExecutor.create(sessionId, sessDef, remoteFs)
          _           <- state.sessionDef.set(Some(sessDef))
          _           <- state.executor.set(Some(newExecutor))
          _           <- newExecutor.start
        } yield ()
      case _                                                                             => ZIO.unit
    }

  private def getExecutor(using state: State, sessionId: String): IO[SessionNotYetStarted, SerialSqlExecutor] =
    for {
      executor <- state.executor.get.someOrFail(SessionNotYetStarted(sessionId))
      _        <- fail(SessionNotYetStarted(sessionId)).whenZIO(executor.isStarted.map(!_))
    } yield executor

  extension (zio: URIO[Sharding, _]) private inline def async: URIO[Sharding & Scope, Unit] = zio.forkScoped.unit
