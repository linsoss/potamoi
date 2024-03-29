package potamoi.flink.interpreter

import com.softwaremill.quicklens.modify
import io.circe.Json
import org.apache.flink.table.api.ResultKind
import org.apache.flink.table.operations.{Operation, QueryOperation, SinkModifyOperation}
import org.apache.flink.table.operations.command.{AddJarOperation, ResetOperation, SetOperation}
import org.apache.flink.table.types.logical.{LogicalTypeRoot, VarCharType}
import org.apache.flink.types.RowKind
import potamoi.{collects, curTs, uuids}
import potamoi.collects.updateWith
import potamoi.flink.FlinkInterpreterErr.*
import potamoi.flink.flinkRest
import potamoi.flink.model.interact.HandleStatus.*
import potamoi.flink.model.interact.ResultDropStrategy.*
import potamoi.flink.model.FlinkTargetType
import potamoi.flink.model.interact.*
import potamoi.fs.RemoteFsOperator
import potamoi.syntax.contra
import potamoi.zios.{runNow, someOrUnit, someOrUnitZIO}
import zio.{Fiber, IO, Promise, Queue, Ref, Scope, UIO, URIO, ZIO}
import zio.ZIO.{attempt, attemptBlocking, attemptBlockingInterrupt, blocking, fail, logInfo, logWarning, succeed, unit}
import zio.ZIOAspect.annotated
import zio.direct.*
import zio.stream.{Stream, ZStream}

import java.util.concurrent.Executors
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.RichOptional

/**
 * Flink sql executor based on client-side execution plan parsing.
 * It would execute all received statements serially.
 */
object SerialSqlExecutor:
  def create(sessionId: String, sessionDef: SessionSpec, remoteFs: RemoteFsOperator): UIO[SerialSqlExecutor] =
    ZIO.succeed(SerialSqlExecutorImpl(sessionId, sessionDef, remoteFs))

trait SerialSqlExecutor:

  def start: URIO[Scope, Unit]
  def stop: UIO[Unit]
  def cancel: UIO[Unit]
  def isStarted: UIO[Boolean]
  def isBusy: UIO[Boolean]

  def completeSql(sql: String, position: Int): UIO[List[String]]
  def completeSql(sql: String): UIO[List[String]]

  def submitSql(sql: String, handleId: String = uuids.genUUID16): IO[ExecuteSqlErr, SqlResult]
  def submitSqlScript(sqlScript: String): IO[SplitSqlScriptErr, SqlScriptResult]

  def submitSqlAsync(sql: String, handleId: String = uuids.genUUID16): UIO[Unit]
  def submitSqlScriptAsync(sqlScript: String): IO[SplitSqlScriptErr, List[ScripSqlSign]]

  def retrieveResultPage(handleId: String, page: Int, pageSize: Int): IO[RetrieveResultNothing, SqlResultPageView]
  def retrieveResultOffset(handleId: String, offset: Long, chunkSize: Int): IO[RetrieveResultNothing, SqlResultOffsetView]

  def listHandleId: UIO[List[String]]
  def listHandleStatus: UIO[List[HandleStatusView]]
  def listHandleFrame: UIO[List[HandleFrame]]

  def getHandleStatus(handleId: String): IO[HandleNotFound, HandleStatusView]
  def getHandleFrame(handleId: String): IO[HandleNotFound, HandleFrame]

/**
 * Default implementation.
 */
class SerialSqlExecutorImpl(sessionId: String, sessionDef: SessionSpec, remoteFs: RemoteFsOperator) extends SerialSqlExecutor:

  type HandleId = String
  private val queryRsLimit: Int                  = sessionDef.resultStore.capacity.contra { limit => if limit < 0 then Integer.MAX_VALUE else limit }
  private val queryRsDropStg: ResultDropStrategy = sessionDef.resultStore.dropStrategy

  private val handleQueue      = Queue.bounded[HandleSign](500).runNow
  private val handleStack      = Ref.make[mutable.Map[HandleId, HandleFrame]](mutable.Map.empty).runNow
  private val lastQueryRsStore = TableRowValueStore.make(queryRsLimit, queryRsDropStg).runNow

  private val handleWorkerFiber = Ref.make[Option[Fiber.Runtime[_, _]]](None).runNow
  private val curHandleFiber    = Ref.make[Option[Fiber.Runtime[_, _]]](None).runNow

  // The executor context for executing the native Flink TableEnvironment api, since
  // TableEnvironment is not thread-safe and can only function properly in a single thread,
  // especially for parsing statement operations.
  private val flinkTableEnvEc                    = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
  private val context                            = Ref.make[Option[SessionContext]](None).runNow
  private def createContext(sessDef: SessionSpec) = SessionContext.buildContext(sessionId, remoteFs, sessDef).onExecutionContext(flinkTableEnvEc)

  sealed private trait HandleSign
  private case class ExecuteSqlCmd(handleId: String, sql: String, promise: Option[Promise[ExecuteSqlErr, SqlResult]]) extends HandleSign
  private case class CompleteSqlCmd(sql: String, position: Int, promise: Promise[Nothing, List[String]])              extends HandleSign
  type ContinueRemaining = Boolean

  sealed private trait OpRsDesc
  private case class PlainRsDesc(result: PlainSqlRs, jobId: Option[String])                                                        extends OpRsDesc
  private case class QueryRsDesc(result: QuerySqlRsDescriptor, jobId: Option[String], collect: Stream[ExecOperationErr, RowValue]) extends OpRsDesc

  /**
   * Start sql handing worker.
   */
  override def start: URIO[Scope, Unit] = {
    for {
      isRunning <- handleWorkerFiber.get.map(_.isDefined)
      _         <- {
        if isRunning then
          ZIO.logInfo(s"Serial sql handle worker is already running, sessionId: $sessionId.") *>
          unit
        else
          ZIO.logInfo(s"Launch serial sql handle worker, sessionId: $sessionId") *>
          handleWorker.forkScoped.flatMap(fiber => handleWorkerFiber.set(Some(fiber)))
      }
    } yield ()
  } @@ annotated("sessionId" -> sessionId)

  /**
   * Stop sql handing worker.
   */
  override def stop: UIO[Unit] = {
    for {
      _ <- logInfo(s"Stop serial sql handle worker: $sessionId")
      _ <- cancel *> handleQueue.shutdown // cancel all handle frame and reply "BeCancelled" to their promise
      _ <- handleWorkerFiber.get.someOrUnit { fiber => fiber.interrupt *> handleWorkerFiber.set(None) }
      _ <- context.get.someOrUnit { ctx => ctx.close *> context.set(None) }
      _ <- handleStack.set(mutable.Map.empty)
      _ <- lastQueryRsStore.clear
    } yield ()
  } @@ annotated("sessionId" -> sessionId)

  /**
   * Cancel the current and remaining handle frame.
   */
  override def cancel: UIO[Unit] = {
    for {
      _ <- logInfo(s"Cancel current handle frames: $sessionId")
      // cancel the remaining handle frames
      _ <- cancelRemainingHandleFrames
      // cancel the current handle frame
      _ <- curHandleFiber.get.someOrUnit { fiber => fiber.interrupt *> curHandleFiber.set(None) }
    } yield ()
  } @@ annotated("sessionId" -> sessionId)

  private def cancelRemainingHandleFrames = {
    ZStream
      .fromIterableZIO(handleQueue.takeAll)
      .runForeach {
        case CompleteSqlCmd(_, _, promise)       => promise.succeed(List.empty)
        case ExecuteSqlCmd(handleId, _, promise) =>
          promise.someOrUnitZIO(_.fail(BeCancelled(handleId))) *>
          handleStack.updateWith(handleId, _.copy(status = Cancel)) *>
          logInfo(s"Handle frame is cancelled: $handleId")
      }
  }

  override def isStarted: UIO[Boolean] = handleWorkerFiber.get.map(_.isDefined)
  override def isBusy: UIO[Boolean]    = curHandleFiber.get.map(_.isDefined)

  /**
   * Flink sql executor bound to a single thread.
   */
  def handleWorker: URIO[Scope, Unit] = {
    handleQueue.take
      .flatMap { sign =>
        for {
          fiber <- (sign match
                     case CompleteSqlCmd(sql, position, promise) =>
                       ZIO.logInfo(s"Handling sql completion command, sql: $sql") *>
                       handleCompleteSqlCmd(sql, position, promise)

                     case ExecuteSqlCmd(handleId, sql, promise) =>
                       // When the current frame execution fails, all subsequent
                       // execution plans that have been received would be cancelled.
                       ZIO.logInfo(s"Handle sql execution command, handleId: ${handleId}, sql: $sql") *>
                       ZIO.scoped {
                         handleExecuteSqlCmd(handleId, sql, promise).flatMap {
                           case true  => unit
                           case false => cancelRemainingHandleFrames
                         }
                       }
                   ).forkScoped
          _     <- curHandleFiber.set(Some(fiber))
          _     <- fiber.join
          _     <- curHandleFiber.set(None)
        } yield ()
      }
      .forever
      .onInterrupt { _ =>
        ZIO.logInfo(s"Serial sql handle worker is interrupted, sessionId: $sessionId.") *> cancel
      }
  } @@ annotated("sessionId" -> sessionId)

  /**
   * Handle completing sql command.
   */
  private def handleCompleteSqlCmd(sql: String, position: Int, promise: Promise[Nothing, List[String]]): UIO[Unit] = {
    val proc = for {
      ctx   <- context.get.someOrElseZIO(createContext(sessionDef).tap(e => context.set(Some(e))))
      hints <- attempt(ctx.parser.getCompletionHints(sql, position))
                 .map(arr => arr.toSeq.toList)
                 .onExecutionContext(flinkTableEnvEc)
      _     <- promise.succeed(hints)
    } yield ()
    proc
      .catchAll(_ => promise.succeed(List.empty).unit)
      .onInterrupt(_ => promise.succeed(List.empty).unit)
  }

  /**
   * Handle executing sql command: initEnv -> parse sql -> execute sql -> reply result.
   */
  private def handleExecuteSqlCmd(
      handleId: String,
      sql: String,
      promise: Option[Promise[ExecuteSqlErr, SqlResult]]): URIO[Scope, ContinueRemaining] = {
    val proc = for {
      // ensure that table environment is initialized
      _         <- handleStack.updateWith(handleId, _.copy(status = Run))
      ctx       <- context.get.someOrElseZIO(createContext(sessionDef).tap(e => context.set(Some(e))))
      bandSinkOp = ctx.sessDef.allowSinkOperation

      // parse and execute sql
      operation  <- attempt(ctx.parser.parse(sql).get(0))
                      .mapError(ParseSqlErr(sql, _))
                      .onExecutionContext(flinkTableEnvEc)
      resultDesc <-
        operation match {
          case op: SetOperation                      => executeSetOperation(ctx, handleId, op)
          case op: ResetOperation                    => executeResetOperation(ctx, handleId, op)
          case op: AddJarOperation                   => executeAddJarOperation(ctx, handleId, op)
          case op: SinkModifyOperation if bandSinkOp => fail(BannedOperation(op.getClass.getName))
          case op                                    => executeOperation(ctx, handleId, op)
        }
      // resolve result and reply promise
      _          <-
        resultDesc match {
          // non-query operation
          case PlainRsDesc(rs, jobId)          =>
            for {
              _ <- handleStack.updateWith(handleId, _.copy(jobId = jobId, status = Finish, result = Some(rs)))
              _ <- promise.someOrUnitZIO(_.succeed(rs))
            } yield ()
          // query operation
          case QueryRsDesc(rs, jobId, collect) =>
            for {
              _           <- handleStack.updateWith(handleId, _.copy(jobId = jobId, status = Run, result = Some(rs)))
              streamChunk <- collect.broadcast(2, 2048)
              workStream   = streamChunk(0)
              watchStream  = streamChunk(1)
              _           <- promise match
                               case None    => watchStream.runDrain.forkScoped
                               case Some(p) => p.succeed(QuerySqlRs(rs, watchStream)) // reply result
              _           <- lastQueryRsStore.bindHandleId(handleId) *> lastQueryRsStore.clear
              _           <- workStream.runDrain
              _           <- handleStack.updateWith(handleId, _.copy(status = Finish))
            } yield ()
        }
    } yield ()

    proc
      .as(true)
      .catchAllCause { cause =>
        // mark status of frame to "Fail" when execution fails.
        ZIO.logErrorCause(s"Fail to handle frame, handleId=$handleId", cause) *>
        handleStack.updateWith(handleId, _.copy(status = Fail, error = Some(HandleErr(cause.failures.head, cause.prettyPrint)))) *>
        promise.someOrUnitZIO(_.failCause(cause)) *>
        succeed(false)
      }
      .onInterrupt { _ =>
        // mark status of frame to "Cancel" when frame has been canceled.
        handleStack.updateWith(handleId, _.copy(status = Cancel)) *>
        promise.someOrUnitZIO(_.fail(BeCancelled(handleId))) *>
        // cancel ref remote flink job if necessary
        cancelRemoteJobIfNecessary(handleId).forkDaemon
      }
    @@ annotated ("handleId" -> handleId)
  }

  private def cancelRemoteJobIfNecessary(handleId: String): UIO[Unit] = defer {
    if sessionDef.execType != FlinkTargetType.Remote || sessionDef.remoteEndpoint.isEmpty then unit.run
    else
      handleStack.get.map(_.get(handleId).flatMap(_.jobId)).run match
        case None        => unit.run
        case Some(jobId) =>
          val endpointUrl = sessionDef.remoteEndpoint.get.contra(ept => s"http://${ept.address}:${ept.port}")
          flinkRest(endpointUrl).cancelJob(jobId).unit.ignore.run
  }

  /**
   * Execute operation that supported by standard Table Environment.
   */
  private def executeOperation(ctx: SessionContext, handleId: String, operation: Operation): ZIO[Scope, ExecOperationErr, OpRsDesc] = {
    for {
      // execute operation
      tableResult <- attempt(ctx.tEnv.executeInternal(operation)).onExecutionContext(flinkTableEnvEc)
      jobId        = tableResult.getJobClient.toScala.map(_.getJobID.toHexString)
      kind         = tableResult.getResultKind
      schema       = tableResult.getResolvedSchema
      colsMeta    <- attempt(FlinkTableResolver.convertResolvedSchema(schema))
      rsConverter <- attempt(FlinkTableResolver.RowDataConverter(schema))
      // collect result
      resultIter  <- ZIO
                       .acquireRelease(attempt(tableResult.collectInternal()))(iter => attempt(iter.close()).ignore)
                       .map(_.asScala)
                       .onExecutionContext(flinkTableEnvEc)
      rsDesc      <-
        operation match {
          case _: QueryOperation =>
            succeed {
              ZStream
                .fromIterator(resultIter, maxChunkSize = 1)
                .mapZIO(rawRow => attempt(rsConverter.convertRow(rawRow))) // convert row format
                .tap(row => lastQueryRsStore.collect(row))                 // collect result
                .mapError(ExecOperationErr(operation.getClass.getName, _))
                .contra { stream =>
                  queryRsDropStg match
                    case DropHead => stream
                    case DropTail => stream.take(queryRsLimit)
                }
            } map { stream =>
              val rs = QuerySqlRsDescriptor(handleId, kind, colsMeta)
              QueryRsDesc(rs, jobId, stream)
            }
          case _                 =>
            ZStream
              .fromIterator(resultIter)
              .mapZIO(rawRow => attempt(rsConverter.convertRow(rawRow))) // convert row format
              .runCollect                                                // collect all rows synchronously
              .map(rows => PlainSqlRs(handleId, kind, colsMeta, rows.toList))
              .map(rs => PlainRsDesc(rs, jobId))
        }
    } yield rsDesc
  }.mapError(ExecOperationErr(operation.getClass.getName, _))

  /**
   * Execute "add jar" sql command, replace the path of jar to local path.
   * See: https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/jar/
   */
  private def executeAddJarOperation(ctx: SessionContext, handleId: String, operation: AddJarOperation): ZIO[Scope, ExecOperationErr, OpRsDesc] =
    for {
      localPath          <- remoteFs.download(operation.getPath).mapBoth(ExecOperationErr(operation.getClass.getName, _), _.getAbsolutePath)
      actualJarOperation <- succeed(AddJarOperation(localPath))
      rsDesc             <- executeOperation(ctx, handleId, actualJarOperation)
    } yield rsDesc

  /**
   * Execute "set" sql command.
   * See: https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/set/
   */
  private def executeSetOperation(
      ctx: SessionContext,
      handleId: String,
      operation: SetOperation): IO[CreateTableEnvironmentErr | ExecOperationErr, OpRsDesc] = {
    val (key, value) = (operation.getKey.toScala.map(_.trim), operation.getValue.toScala.map(_.trim))

    (key, value) match
      // update env
      case (Some(k), Some(v)) if k.nonEmpty =>
        updateEnv(ctx, Map(k -> v)).mapError(ExecOperationErr(operation.getClass.getName, _)) *>
        succeed {
          PlainRsDesc(result = PlainSqlRs.plainOkResult(handleId), jobId = None)
        }

      // show configuration
      case _ =>
        succeed(ctx.configuration.toMap.asScala).map { configMap =>
          PlainRsDesc(
            result = PlainSqlRs(
              handleId = handleId,
              kind = ResultKind.SUCCESS_WITH_CONTENT,
              columns = List(FieldMeta("key", LogicalTypeRoot.VARCHAR, "STRING"), FieldMeta("value", LogicalTypeRoot.VARCHAR, "STRING")),
              data = configMap.map { case (k, v) =>
                RowValue(RowKind.INSERT, Json.fromValues(Seq(Json.fromString(k), Json.fromString(v))))
              }.toList
            ),
            jobId = None)
        }
  }

  /**
   * Execute "reset" sql command.
   * See: https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/set/
   */
  private def executeResetOperation(
      ctx: SessionContext,
      handleId: String,
      operation: ResetOperation): IO[CreateTableEnvironmentErr | ExecOperationErr, OpRsDesc] = {
    operation.getKey.toScala.map(_.trim) match
      // reset configuration via key
      case Some(key) if key.nonEmpty =>
        val reset = sessionDef.extraProps.get(key) match
          case None           => unit
          case Some(oriValue) => updateEnv(ctx, Map(key -> oriValue))
        reset *> succeed(PlainRsDesc(PlainSqlRs.plainOkResult(handleId), None))
      // reset whole env
      case _                         =>
        resetEnv(ctx) *>
        succeed(PlainRsDesc(PlainSqlRs.plainOkResult(handleId), None))
  }

  /**
   * Update raw configuration of environment.
   */
  private def updateEnv(ctx: SessionContext, configs: Map[String, String]): IO[CreateTableEnvironmentErr, Unit] = {
    for {
      newSessDef <- succeed(ctx.sessDef.modify(_.extraProps)(_ ++ configs))
      _          <- ctx.close
      newCtx     <- createContext(newSessDef)
      _          <- context.set(Some(newCtx))
    } yield ()
  }

  /**
   * Reset environment to the original one.
   */
  private def resetEnv(ctx: SessionContext): IO[CreateTableEnvironmentErr, Unit] = {
    if ctx.sessDef == sessionDef then unit
    else
      for {
        _      <- ctx.close
        newCtx <- createContext(sessionDef)
        _      <- context.set(Some(newCtx))
      } yield ()
  }

  /**
   * Submit sql statement and wait for the execution result.
   */
  override def submitSql(sql: String, handleId: String = uuids.genUUID16): IO[ExecuteSqlErr, SqlResult] = {
    for {
      promise <- Promise.make[ExecuteSqlErr, SqlResult]
      _       <- submitSqlInternal(sql, handleId, Some(promise))
      reply   <- blocking(promise.await)
    } yield reply
  } @@ annotated("sessionId" -> sessionId)

  /**
   * Submit sql statement async.
   */
  override def submitSqlAsync(sql: HandleId, handleId: HandleId): UIO[Unit] = {
    submitSqlInternal(sql, handleId, None) @@ annotated("handleId" -> handleId)
  }

  private def submitSqlInternal(sql: String, handleId: String, promise: Option[Promise[ExecuteSqlErr, SqlResult]]): UIO[Unit] = {
    for
      _ <- handleStack.update(_ += handleId -> HandleFrame(handleId, sql, status = Wait))
      _ <- handleQueue.offer(ExecuteSqlCmd(handleId, sql, promise))
    yield ()
  }

  /**
   * Submit sql script and wait for the execution result.
   */
  override def submitSqlScript(sqlScript: HandleId): IO[SplitSqlScriptErr, SqlScriptResult] = {
    for
      sqls       <- FlinkSqlTool.splitSqlScript(sqlScript).mapError(SplitSqlScriptErr.apply)
      sqlSigns    = sqls.map(sql => ScripSqlSign(uuids.genUUID16, sql))
      promises   <- ZIO.foreach(sqlSigns) { sign =>
                      Promise
                        .make[ExecuteSqlErr, SqlResult]
                        .tap(promise => submitSqlInternal(sign.sql, sign.handleId, Some(promise)))
                    }
      watchStream = ZStream.fromIterable(promises).mapZIO(promise => blocking(promise.await))
    yield SqlScriptResult(sqlSigns, watchStream)
  } @@ annotated("sessionId" -> sessionId)

  /**
   * Submit sql script.
   */
  override def submitSqlScriptAsync(sqlScript: HandleId): IO[SplitSqlScriptErr, List[ScripSqlSign]] = {
    for
      sqls    <- FlinkSqlTool.splitSqlScript(sqlScript).mapError(SplitSqlScriptErr.apply)
      sqlSigns = sqls.map(sql => ScripSqlSign(uuids.genUUID16, sql))
      _       <- ZIO.foreach(sqlSigns)(sign => submitSqlInternal(sign.sql, sign.handleId, None))
    yield sqlSigns
  } @@ annotated("sessionId" -> sessionId)

  /**
   * Get completion hints for the given statement at the given cursor position.
   */
  override def completeSql(sql: HandleId, position: Int): UIO[List[HandleId]] = {
    for
      promise <- Promise.make[Nothing, List[HandleId]]
      _       <- handleQueue.offer(CompleteSqlCmd(sql, position, promise))
      hints   <- promise.await
    yield hints
  } @@ annotated("sessionId" -> sessionId)

  override def completeSql(sql: HandleId): UIO[List[HandleId]] = completeSql(sql, sql.length)

  /**
   * Returns sql results in a paged manner.
   *
   * @param page is from 1 on.
   */
  override def retrieveResultPage(handleId: String, page: Int, pageSize: Int): IO[RetrieveResultNothing, SqlResultPageView] = {
    handleStack.get.map(_.get(handleId)).flatMap {
      case None        => fail(HandleNotFound(handleId))
      case Some(frame) =>
        frame.result match
          case None                           => fail(ResultNotFound(handleId))
          case Some(rs: PlainSqlRs)           => succeed(SqlResultPageView(rs))
          case Some(rs: QuerySqlRsDescriptor) =>
            defer {
              val lastQueryHandleId = lastQueryRsStore.handleId.run
              if lastQueryHandleId != rs.handleId then succeed(SqlResultPageView(PlainSqlRs(rs, List.empty))).run
              else {
                val rowsSnapshot       = lastQueryRsStore.snapshot.run
                val totalPage          = (rowsSnapshot.size / pageSize.toDouble).ceil.toInt
                val offset             = pageSize * (page - 1)
                val rows               = rowsSnapshot.slice(offset, pageSize)
                val hasNextPage        = if page < totalPage then true else frame.status == Run
                val hasNextRowThisPage = if page < totalPage then false else frame.status == Run
                succeed(SqlResultPageView(totalPage, page, hasNextPage, hasNextRowThisPage, PlainSqlRs(rs, rows))).run
              }
            }
    }
  } @@ annotated("sessionId" -> sessionId, "handleId" -> handleId)

  /**
   * Return sql results as row minimum timestamp offset.
   */
  override def retrieveResultOffset(handleId: HandleId, offset: Long, chunkSize: Int): IO[RetrieveResultNothing, SqlResultOffsetView] = {
    handleStack.get.map(_.get(handleId)).flatMap {
      case None        => fail(HandleNotFound(handleId))
      case Some(frame) =>
        frame.result match
          case None                           => fail(ResultNotFound(handleId))
          case Some(rs: PlainSqlRs)           => succeed(SqlResultOffsetView(rs.data.lastOption.map(_.nanoTs).getOrElse(curTs), false, rs))
          case Some(rs: QuerySqlRsDescriptor) =>
            defer {
              val lastQueryHandleId = lastQueryRsStore.handleId.run
              if lastQueryHandleId != rs.handleId then succeed(SqlResultOffsetView(PlainSqlRs(rs, List.empty))).run
              else {
                val rowsSnapshot = lastQueryRsStore.snapshot.run
                val rows         = rowsSnapshot.filter(_.nanoTs > offset).take(chunkSize)
                succeed(
                  SqlResultOffsetView(
                    lastOffset = rows.lastOption.map(_.nanoTs).getOrElse(offset),
                    hasNextRow = frame.status == Run,
                    payload = PlainSqlRs(rs, rows)
                  )).run
              }
            }
    }
  } @@ annotated("sessionId" -> sessionId, "handleId" -> handleId)

  /**
   * List all handle ids, order by submit time asc.
   */
  override def listHandleId: UIO[List[String]] = {
    handleStack.get.map { stack =>
      stack.values.toList.sortBy(_.submitAt).map(_.handleId)
    }
  } @@ annotated("sessionId" -> sessionId)

  /**
   * List all handle status, order by submit time asc.
   */
  override def listHandleStatus: UIO[List[HandleStatusView]] = {
    handleStack.get.map { stack =>
      stack.values.toList
        .sortBy(_.submitAt)
        .map(e => HandleStatusView(e.handleId, e.status, e.submitAt))
    }
  } @@ annotated("sessionId" -> sessionId)

  /**
   * Get handle status of given handleId
   */
  override def getHandleStatus(handleId: HandleId): IO[HandleNotFound, HandleStatusView] = {
    handleStack.get
      .map(_.get(handleId).map(e => HandleStatusView(e.handleId, e.status, e.submitAt)))
      .someOrFail(HandleNotFound(handleId))
  } @@ annotated("sessionId" -> sessionId)

  /**
   * Get HandleFrame of given handle id.
   */
  override def getHandleFrame(handleId: String): IO[HandleNotFound, HandleFrame] = {
    handleStack.get
      .map(_.get(handleId))
      .someOrFail(HandleNotFound(handleId))
  } @@ annotated("sessionId" -> sessionId)

  /**
   * List all HandleFrame, order by submit time asc.
   */
  override def listHandleFrame: UIO[List[HandleFrame]] = {
    handleStack.get
      .map(_.values.toList.sortBy(_.submitAt))
  } @@ annotated("sessionId" -> sessionId)
