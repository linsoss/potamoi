package potamoi.flink.interp

import com.softwaremill.quicklens.modify
import io.circe.Json
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.api.ResultKind
import org.apache.flink.table.delegation.Parser
import org.apache.flink.table.operations.*
import org.apache.flink.table.operations.command.{AddJarOperation, ResetOperation, SetOperation}
import org.apache.flink.table.operations.ddl.{AlterOperation, CreateOperation, DropOperation}
import org.apache.flink.table.types.logical.{LogicalTypeRoot, VarCharType}
import org.apache.flink.types.RowKind
import potamoi.{curTs, uuids}
import potamoi.collects.updateWith
import potamoi.common.Uuid
import potamoi.common.ZIOExtension.zioRun
import potamoi.errs.{headMessage, recurse}
import potamoi.flink.{FlinkConf, FlinkErr, FlinkInterpErr}
import potamoi.flink.FlinkConfigurationTool.safeSet
import potamoi.flink.FlinkInterpErr.{InitSessionFail, SessionNotReady}
import potamoi.flink.interp.model.*
import potamoi.flink.interp.FlinkTableResolver.RowDataConverter
import potamoi.flink.interp.model.ResultDropStrategy.{DropHead, DropTail}
import potamoi.fs.refactor.{paths, RemoteFsOperator}
import potamoi.syntax.{contra, tap, valueToSome}
import zio.{IO, Queue, Ref, Scope, Task, UIO, Unsafe, ZIO}
import zio.direct.*
import zio.ZIO.{attempt, succeed, unit}
import zio.stream.ZStream

import java.net.URLClassLoader
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.RichOptional

type SqlContent = String

/**
 * Flink sql executor based on client-side execution plan parsing.
 */
object SqlExecutor:

  def instance(sessionId: String, remoteFs: RemoteFsOperator): UIO[SqlExecutor] =
    for {
      oriSessDef   <- Ref.make[Option[SessionDef]](None)
      context      <- Ref.make[Option[SessionContext]](None)
      handleQueue  <- Queue.unbounded[(HandleId, SqlContent)]
      handleFrames <- Ref.make[mutable.Map[HandleId, HandleFrame]](mutable.Map.empty)
      sqlResults   <- Ref.make[mutable.Map[HandleId, SqlRsView]](mutable.Map.empty)
    } yield SqlExecutor(sessionId, remoteFs)(oriSessDef, context, handleQueue, handleFrames, sqlResults)

class SqlExecutor(
    sessionId: String,
    remoteFs: RemoteFsOperator
  )(oriSessDef: Ref[Option[SessionDef]],
    context: Ref[Option[SessionContext]],
    handleQueue: Queue[(HandleId, SqlContent)],
    handleFrames: Ref[mutable.Map[HandleId, HandleFrame]],
    sqlResults: Ref[mutable.Map[HandleId, SqlRsView]]):

  /**
   * Initialize environment.
   */
  def initEnvironment(sessDef: SessionDef): ZIO[Scope, InitSessionFail, Unit] = for {
    ctx <- createContext(sessDef).mapError(InitSessionFail(sessionId, _))
    _   <- oriSessDef.set(Some(sessDef))
    _   <- context.set(Some(ctx))
    // launch statement handing fibers
    _ <- handleQueue.take.flatMap { case (handleId, sqlContent) => handleStatement(handleId, sqlContent) }.forever.forkScoped
  } yield ()

  /**
   * Update raw configuration of environment.
   */
  private def updateEnvironment(configs: Map[String, String]): IO[SessionNotReady | InitSessionFail, Unit] = for {
    ctx <- context.get.someOrFail(SessionNotReady(sessionId))
    newSessDef = ctx.sessDef.modify(_.extraProps)(_ ++ configs)
    _      <- ctx.close
    newCtx <- createContext(newSessDef).mapError(InitSessionFail(sessionId, _))
    _      <- context.set(Some(newCtx))
  } yield ()

  /**
   * Reset environment to the original one.
   */
  private def resetEnvironment: IO[Throwable, Unit] = for {
    ctx           <- context.get.someOrFail(SessionNotReady(sessionId))
    originSessDef <- oriSessDef.get.someOrFail(SessionNotReady(sessionId))
    _ <-
      if ctx.sessDef == originSessDef then unit
      else
        ctx.close *>
        createContext(originSessDef)
          .mapError(InitSessionFail(sessionId, _))
          .flatMap(r => context.set(Some(r)))
  } yield ()

  private def createContext(sessDef: SessionDef) = SessionContext.buildContext(sessionId, sessDef, remoteFs)

  /**
   * Terminate executor.
   */
  def terminate: UIO[Unit] =
    for {
      _ <- handleQueue.shutdown
      _ <- context.get.flatMap {
        case None      => unit
        case Some(ctx) => ctx.close
      }
    } yield ()

  /**
   * Submit sql content.
   */
  def submitStatement(sqlContent: String): UIO[HandleId] =
    for {
      handleId <- ZIO.succeed(uuids.genUUID16)
      _        <- handleQueue.offer(handleId -> sqlContent)
      _        <- handleFrames.update(_ += uuids.genUUID16 -> HandleFrame(sql = sqlContent, state = HandleStatus.Wait, submitAt = curTs))
    } yield handleId

  /**
   * Parse sql content and then execute it.
   */
  private def handleStatement(handleId: HandleId, sqlContent: SqlContent): UIO[Unit] = {
    for {
      ctx       <- context.get.someOrFail(SessionNotReady(sessionId))
      _         <- handleFrames.updateWith(handleId, _.copy(state = HandleStatus.Run, runAt = curTs))
      operation <- attempt(ctx.parser.parse(sqlContent).get(0))
      _         <- StmtExecutor(handleId, operation, ctx).active
      _         <- handleFrames.updateWith(handleId, _.copy(state = HandleStatus.Finish, endAt = curTs))
    } yield ()
  } catchAllCause { cause =>
    // cancel all frames that are waiting to be executed.
    for {
      errDetail     <- succeed(ErrorDetail(cause.headMessage, cause.prettyPrint))
      _             <- handleFrames.updateWith(handleId, _.copy(state = HandleStatus.Fail, endAt = curTs, error = Some(errDetail)))
      waitingFrames <- handleQueue.takeAll.map(_.map(_._1).filter(_ != handleId))
      _             <- ZStream.fromChunk(waitingFrames).mapZIO(id => handleFrames.updateWith(id, _.copy(state = HandleStatus.Cancel))).runDrain
    } yield ()
  }

  /**
   * Flink statement executor.
   */
  private class StmtExecutor(handleId: HandleId, operation: Operation, ctx: SessionContext) {

    private lazy val rsLimit   = ctx.sessDef.resultStore.capacity
    private lazy val rsDropStg = ctx.sessDef.resultStore.dropStrategy

    def active = operation match
      case op: SetOperation    => execSetOp(op)
      case op: ResetOperation  => execResetOp(op)
      case op: AddJarOperation => execAddJarOp(op)
      case op                  => execOp(op)

    /**
     * Add jar operation, replace the path of jar to local path.
     * See: https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/jar/]
     */
    private def execAddJarOp(op: AddJarOperation): Task[Unit] =
      for {
        localPath          <- remoteFs.download(op.getPath).map(_.getAbsolutePath)
        actualJarOperation <- succeed(AddJarOperation(localPath))
        _                  <- execOp(actualJarOperation)
      } yield ()

    /**
     * Set operation.
     * See: https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/set/
     */
    private def execSetOp(op: SetOperation): Task[Unit] = {

      // update environment
      def updateEnv(key: String, value: String) = for {
        _ <- updateEnvironment(Map(key -> value))
        _ <- sqlResults.update(_ += handleId -> SqlRsView.plainOkResult)
      } yield ()

      // show flink configuration
      def showConfig = for {
        configMap <- succeed(ctx.configuration.toMap.asScala)
        rsView = SqlRsView(
          kind = ResultKind.SUCCESS_WITH_CONTENT,
          columns = List(FieldMeta("key", LogicalTypeRoot.VARCHAR, "STRING"), FieldMeta("value", LogicalTypeRoot.VARCHAR, "STRING")),
          data = ListBuffer.from(
            configMap.map { case (k, v) => RowValue(RowKind.INSERT, Json.fromValues(Seq(Json.fromString(k), Json.fromString(v)))) }
          ))
        _ <- sqlResults.update(_ += handleId -> rsView)
      } yield ()

      (op.getKey.toScala.map(_.trim), op.getValue.toScala.map(_.trim)) match {
        case (Some(key), Some(value)) if key.nonEmpty => updateEnv(key, value)
        case _                                        => showConfig
      }
    }

    /**
     * Reset operation.
     * See: https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/reset/
     */
    private def execResetOp(op: ResetOperation): Task[Unit] = {
      op.getKey.toScala.map(_.trim) match {
        // reset configuration via key
        case Some(key) if key.nonEmpty =>
          for {
            originSessDef <- oriSessDef.get.map(_.getOrElse(ctx.sessDef))
            oriConfValue = originSessDef.extraProps.get(key)
            _ <- updateEnvironment(Map(key -> oriConfValue.get)).when(oriConfValue.isDefined)
          } yield ()
        // reset whole env
        case _ => resetEnvironment
      }
    }

    /**
     * Execute operation that supported by standard Table Environment.
     */
    private def execOp(op: Operation): Task[Unit] = for {
      // execute operation
      tableResult <- attempt(ctx.tEnv.executeInternal(op))
      jobId  = tableResult.getJobClient.toScala.map(_.getJobID.toHexString)
      kind   = tableResult.getResultKind
      schema = tableResult.getResolvedSchema
      colsMeta    <- attempt(FlinkTableResolver.convertResolvedSchema(schema))
      rsConverter <- attempt(RowDataConverter(schema))

      // update state
      _ <- handleFrames.updateWith(handleId, _.copy(jobId = jobId))
      _ <- sqlResults.update(_ += handleId -> SqlRsView(kind = kind, columns = colsMeta))

      // collect result
      rsIterator <- attempt(tableResult.collectInternal().asScala)
      _ <- ZStream
        .fromIterator(rsIterator)
        .mapZIO(row => attempt(rsConverter.convertRow(row)))
        .mapZIO(row => sqlResults.updateWith(handleId, view => { mergeRow(row, view.data); view }))
        .contra { stream =>
          rsDropStg match
            case DropHead => stream.runDrain
            case DropTail => stream.take(rsLimit).runDrain
        }
    } yield ()

    private lazy val mergeRow: (RowValue, ListBuffer[RowValue]) => ListBuffer[RowValue] = { (row, data) =>
      rsDropStg match
        case DropHead => if data.size < rsLimit then data :+ row else (data -= data.head) :+ row
        case DropTail => data :+ row
      data
    }
  }

end SqlExecutor
