package potamoi.flink.interp

import com.softwaremill.quicklens.modify
import io.circe.Json
import org.apache.flink.table.api.ResultKind
import org.apache.flink.table.operations.{Operation, QueryOperation, SinkModifyOperation}
import org.apache.flink.table.operations.command.{AddJarOperation, ResetOperation, SetOperation}
import org.apache.flink.table.types.logical.{LogicalTypeRoot, VarCharType}
import org.apache.flink.types.RowKind
import potamoi.{curTs, uuids}
import potamoi.flink.interp.model.*
import potamoi.flink.interp.FlinkInterpErr.*
import potamoi.flink.interp.FlinkTableResolver.RowDataConverter
import potamoi.flink.interp.model.ResultDropStrategy.{DropHead, DropTail}
import potamoi.fs.refactor.RemoteFsOperator
import potamoi.syntax.{contra, valueToSome}
import potamoi.zios.runNow
import zio.{Fiber, IO, Ref, Scope, Task, UIO, ZIO, ZLayer}
import zio.stream.{Stream, ZStream}
import zio.ZIO.{attempt, fail, succeed, unit}
import zio.direct.*

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.RichOptional

/**
 * Flink sql executor based on client-side execution plan parsing.
 * It would execute all received statements serially.
 */
trait SerialSqlExecutor:

  def initEnv(sessDef: SessionDef): ZIO[Scope, InitEnvErr, Unit]
  def closeEnv: UIO[Unit]

  def completeSql(sql: String, position: Int): UIO[List[String]]
  def executeSql(sql: String): ZIO[Scope, ExecuteSqlErr, SqlResult]
  def retrieveResultPage(handleId: String, page: Int, pageSize: Int): IO[HandleFound.type, SqlResultPage]

  def getCurHandle: UIO[Option[HandleFrame]]
  def cancelCurHandle: UIO[Unit]
  def listHandle: UIO[List[String]]
  def getHandleFrame(handleId: String): IO[HandleFound.type, HandleFrame]
  def listHistory: UIO[List[HistHandleFrame]]

/**
 * Default implementation.
 */
class SerialSqlExecutorImpl(sessionId: String, remoteFs: RemoteFsOperator) extends SerialSqlExecutor:

  private type Frame = (HandleFrame, Option[Fiber.Runtime[_, Unit]])

  private val oriSessDef           = Ref.make[Option[SessionDef]](None).runNow
  private val context              = Ref.make[Option[SessionContext]](None).runNow
  private val curHandleFrame       = Ref.make[Option[Frame]](None).runNow
  private val histHandleFrameStack = Ref.make[ListBuffer[HistHandleFrame]](ListBuffer.empty).runNow

  private val lastQueryHandleRsDesc  = Ref.make[Option[QuerySqlRsDescriptor]](None).runNow
  private val lastQueryHandleRsStore = Ref.make[ListBuffer[RowValue]](ListBuffer.empty).runNow

  private def createContext(sessDef: SessionDef) = SessionContext.buildContext(sessionId, sessDef, remoteFs)

  /**
   * Initialize environment.
   */
  override def initEnv(sessDef: SessionDef): ZIO[Scope, InitEnvErr, Unit] = for {
    _   <- stopCurHandleFrame
    _   <- context.get.flatMap(_.map(_.close).getOrElse(unit))
    ctx <- createContext(sessDef).mapError(InitEnvErr.apply)
    _   <- context.set(ctx)
    _   <- oriSessDef.set(sessDef)
  } yield ()

  /**
   * Close environment and terminate executor.
   */
  override def closeEnv: UIO[Unit] = for {
    _ <- stopCurHandleFrame
    _ <- context.get.flatMap(_.map(_.close).getOrElse(unit))
    _ <- context.set(None)
  } yield ()

  private def stopCurHandleFrame: UIO[Unit] = for {
    _ <- curHandleFrame.get.flatMap {
           case Some((_, Some(fiber))) => fiber.interrupt
           case _                      => unit
         }
    _ <- curHandleFrame.set(None)
  } yield ()

  /**
   * Get completion hints for the given statement at the given cursor position.
   */
  override def completeSql(sql: String, position: Int): UIO[List[String]] = {
    for {
      ctx   <- context.get.someOrFail(EnvNotYetReady)
      hints <- attempt(ctx.parser.getCompletionHints(sql, position)).map(e => e.toSeq.toList)
    } yield hints
  } catchAll (_ => succeed(List.empty))

  /**
   * Parse given statement and execute it, returns table result or result descriptor
   * for QueryOperation.
   */
  override def executeSql(sql: String): ZIO[Scope, ExecuteSqlErr, SqlResult] = {
    for {
      ctx        <- context.get.someOrFail(EnvNotYetReady)
      _          <- curHandleFrame.get.flatMap {
                      case Some(frame) => fail(ExecutorBusy(frame._1.handleId))
                      case None        => unit
                    }
      handleId    = uuids.genUUID16
      handleFrame = HandleFrame(handleId, sql, HandleStatus.Run)
      _          <- curHandleFrame.set(Some(handleFrame -> None))

      // parse and execute
      operation <- attempt(ctx.parser.parse(sql).get(0)).mapError(ParseSqlErr(sql, _))
      rsDesc    <-
        operation match
          case op: SetOperation                                           => executeSetOperation(ctx, handleId, op)
          case op: ResetOperation                                         => executeResetOperation(ctx, handleId, op)
          case op: AddJarOperation                                        => executeAddJarOperation(ctx, handleId, op)
          case op: SinkModifyOperation if !ctx.sessDef.allowSinkOperation => fail(BannedOperation(op.getClass.getName))
          case op                                                         => executeOperation(ctx, handleId, op)
      // resolve handle frame state
      result    <-
        rsDesc match {
          // non-query operation result
          case PlainSqlRsDesc(rs, jobId)         =>
            val histFrame = HistHandleFrame(frame = handleFrame.copy(status = HandleStatus.Finish, jobId = jobId), result = rs)
            curHandleFrame.set(None) *>
            histHandleFrameStack.update(_ += histFrame) *>
            succeed(rs)

          // query operation result
          case QuerySqlRsDesc(rs, jobId, stream) =>
            val curFrame = handleFrame.copy(jobId = jobId)
            val effect   = {
              lastQueryHandleRsDesc.set(Some(rs)) *>
              lastQueryHandleRsStore.set(ListBuffer.empty) *>
              stream.runDrain *>
              curHandleFrame.set(None) *>
              histHandleFrameStack.update(_ += HistHandleFrame(frame = handleFrame.copy(status = HandleStatus.Finish), result = rs))
            } catchAll { err =>
              curHandleFrame.set(None) *>
              histHandleFrameStack.update(_ += HistHandleFrame(frame = handleFrame.copy(status = HandleStatus.Fail), result = rs, error = Some(err)))
            }
            for {
              watchStream <- stream.broadcastDynamic(1024)
              _           <- effect.forkScoped.flatMap(fiber => curHandleFrame.set(Some(curFrame -> fiber)))
            } yield QuerySqlRs(rs, watchStream)
        }
    } yield result
  }.tapError { _ => curHandleFrame.set(None) }

  sealed private trait OpRsDesc
  private case class PlainSqlRsDesc(result: PlainSqlRs, jobId: Option[String] = None) extends OpRsDesc
  private case class QuerySqlRsDesc(result: QuerySqlRsDescriptor, jobId: Option[String] = None, collect: Stream[ExecOperationErr, RowValue])
      extends OpRsDesc

  /**
   * Execute operation that supported by standard Table Environment.
   */
  private def executeOperation(ctx: SessionContext, handleId: String, operation: Operation): IO[ExecOperationErr, OpRsDesc] = {
    lazy val rsLimit   = ctx.sessDef.resultStore.capacity.contra { limit => if limit < 0 then Integer.MAX_VALUE else limit }
    lazy val rsDropStg = ctx.sessDef.resultStore.dropStrategy

    def collectQueryOpRs(row: RowValue): UIO[Unit] = lastQueryHandleRsStore.update { rows =>
      rsDropStg match
        case DropHead => if rows.size < rsLimit then rows += row else (rows -= rows.head) += row
        case DropTail => rows += row
    }

    for {
      // execute operation
      tableResult <- attempt(ctx.tEnv.executeInternal(operation))
      jobId        = tableResult.getJobClient.toScala.map(_.getJobID.toHexString)
      kind         = tableResult.getResultKind
      schema       = tableResult.getResolvedSchema
      colsMeta    <- attempt(FlinkTableResolver.convertResolvedSchema(schema))
      rsConverter <- attempt(RowDataConverter(schema))
      // collect result
      rsDesc      <-
        operation match {
          case _: QueryOperation =>
            succeed {
              ZStream
                .fromIteratorZIO(attempt(tableResult.collectInternal().asScala))
                .mapZIO(rawRow => attempt(rsConverter.convertRow(rawRow)))
                .tap(row => collectQueryOpRs(row))
                .mapError(ExecOperationErr(operation.getClass.getName, _))
                .contra { stream =>
                  rsDropStg match
                    case DropHead => stream
                    case DropTail => stream.take(rsLimit)
                }
            } map { stream =>
              val rs = QuerySqlRsDescriptor(handleId, kind, colsMeta)
              QuerySqlRsDesc(rs, jobId, stream)
            }
          case _                 =>
            ZStream
              .fromIteratorZIO(attempt(tableResult.collectInternal().asScala))
              .mapZIO(rawRow => attempt(rsConverter.convertRow(rawRow)))
              .runCollect
              .map(rows => PlainSqlRs(handleId, kind, colsMeta, rows.toList))
              .map(rs => PlainSqlRsDesc(rs, jobId))
        }
    } yield rsDesc
  }.mapError(ExecOperationErr(operation.getClass.getName, _))

  /**
   * Execute "add jar" sql command, replace the path of jar to local path.
   * See: https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/jar/
   */
  private def executeAddJarOperation(
      ctx: SessionContext,
      handleId: String,
      operation: AddJarOperation): IO[PotaInternalErr | ExecOperationErr, OpRsDesc] = for {
    localPath          <- remoteFs.download(operation.getPath).mapBoth(PotaInternalErr(_), _.getAbsolutePath)
    actualJarOperation <- succeed(AddJarOperation(localPath))
    rsDesc             <- executeOperation(ctx, handleId, actualJarOperation)
  } yield rsDesc

  /**
   * Execute "set" sql command.
   * See: https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/set/
   */
  private def executeSetOperation(ctx: SessionContext, handleId: String, operation: SetOperation): IO[PotaInternalErr, OpRsDesc] = {
    val (key, value) = (operation.getKey.toScala.map(_.trim), operation.getValue.toScala.map(_.trim))
    (key, value) match
      // update env
      case (Some(k), Some(v)) if k.nonEmpty =>
        updateEnv(Map(k -> v)).mapError(PotaInternalErr(_)) *> succeed(PlainSqlRsDesc(PlainSqlRs.plainOkResult(handleId)))

      // show configuration
      case _ =>
        succeed(ctx.configuration.toMap.asScala).map { configMap =>
          val rs = PlainSqlRs(
            handleId = handleId,
            kind = ResultKind.SUCCESS_WITH_CONTENT,
            columns = List(FieldMeta("key", LogicalTypeRoot.VARCHAR, "STRING"), FieldMeta("value", LogicalTypeRoot.VARCHAR, "STRING")),
            data = configMap.map { case (k, v) =>
              RowValue(RowKind.INSERT, Json.fromValues(Seq(Json.fromString(k), Json.fromString(v))))
            }.toList
          )
          PlainSqlRsDesc(rs)
        }
  }

  private def executeResetOperation(
      ctx: SessionContext,
      handleId: String,
      operation: ResetOperation): IO[PotaInternalErr, OpRsDesc] = {
    operation.getKey.toScala.map(_.trim) match
      // reset configuration via key
      case Some(key) if key.nonEmpty =>
        for {
          originSessDef <- oriSessDef.get.map(_.getOrElse(ctx.sessDef))
          oriConfValue   = originSessDef.extraProps.get(key)
          _             <- updateEnv(Map(key -> oriConfValue.get)).when(oriConfValue.isDefined).mapError(PotaInternalErr(_))
        } yield PlainSqlRsDesc(PlainSqlRs.plainOkResult(handleId))
      // reset whole env
      case _                         =>
        resetEnv.mapError(PotaInternalErr(_)) *>
        succeed(PlainSqlRsDesc(PlainSqlRs.plainOkResult(handleId)))
  }

  /**
   * Update raw configuration of environment.
   */
  private def updateEnv(configs: Map[String, String]): IO[EnvNotYetReady.type | InitEnvErr, Unit] = for {
    ctx       <- context.get.someOrFail(EnvNotYetReady)
    newSessDef = ctx.sessDef.modify(_.extraProps)(_ ++ configs)
    _         <- ctx.close
    newCtx    <- createContext(newSessDef).mapError(InitEnvErr.apply)
    _         <- context.set(newCtx)
  } yield ()

  /**
   * Reset environment to the original one.
   */
  private def resetEnv: IO[EnvNotYetReady.type | InitEnvErr, Unit] = for {
    ctx           <- context.get.someOrFail(EnvNotYetReady)
    originSessDef <- oriSessDef.get.someOrFail(EnvNotYetReady)
    _             <-
      if ctx.sessDef == originSessDef then unit
      else
        ctx.close *>
        createContext(originSessDef)
          .mapError(InitEnvErr.apply)
          .flatMap(r => context.set(r))
  } yield ()

  /**
   * Returns the sql results in a paged manner.
   * @param page is from 1 on.
   */
  override def retrieveResultPage(handleId: String, page: Int, pageSize: Int): IO[HandleFound.type, SqlResultPage] =
    lastQueryHandleRsDesc.get.flatMap {
      // from last query handle result
      case Some(desc) if desc.handleId == handleId =>
        lastQueryHandleRsStore.get.map(_.toList).flatMap { rs =>
          val totalPage = (rs.size / pageSize.toDouble).ceil.toInt
          val offset    = pageSize * (page - 1)
          val rows      = rs.slice(offset, pageSize)
          succeed(SqlResultPage(totalPage, page, pageSize, PlainSqlRs(desc, rows)))
        }

      // from history result`
      case _ =>
        histHandleFrameStack.get.flatMap { histStack =>
          histStack.find(e => e.frame.handleId == handleId) match
            case None       => fail(HandleFound)
            case Some(hist) =>
              hist.result match
                case r: PlainSqlRs           => succeed(SqlResultPage(1, page, pageSize, r))
                case r: QuerySqlRsDescriptor => succeed(SqlResultPage(1, page, pageSize, PlainSqlRs(r, List.empty)))
        }
    }

  /**
   * Cancel the sql execution handler.
   */
  override def cancelCurHandle: UIO[Unit] = {
    (curHandleFrame.get <&> lastQueryHandleRsDesc.get).flatMap {
      case (Some((frame, Some(fiber))), Some(desc)) =>
        fiber.interrupt *>
        curHandleFrame.set(None) *>
        histHandleFrameStack
          .update(_ += HistHandleFrame(frame = frame.copy(status = HandleStatus.Cancel), result = desc))
          .when(desc.handleId == frame.handleId)
          .unit
      case _                                        => unit
    }
  }

  /**
   * List all handle ids, order by submit time asc.
   */
  override def listHandle: UIO[List[String]] = {
    val histHandleIds = histHandleFrameStack.get.map(_.map(_.frame.handleId).toList)
    val curHandleId   = curHandleFrame.get.map(_.map(_._1.handleId).toList)
    (histHandleIds <&> curHandleId) map { case (hist, cur) => (hist ++ cur).distinct }
  }

  /**
   * Get current running sql handle frame.
   */
  override def getCurHandle: UIO[Option[HandleFrame]] = curHandleFrame.get.map(_.map(_._1))

  /**
   * Get status of given handleId.
   */
  override def getHandleFrame(handleId: String): IO[HandleFound.type, HandleFrame] = {
    val findCur  = curHandleFrame.get.map(_.filter(_._1.handleId == handleId).map(_._1))
    val findHist = histHandleFrameStack.get.map(hist => hist.find(_.frame.handleId == handleId).map(_.frame))
    (findCur <&> findHist).flatMap {
      case (None, None)     => fail(HandleFound)
      case (Some(frame), _) => succeed(frame)
      case (_, Some(frame)) => succeed(frame)
    }
  }

  /**
   * Lists history handle frames.
   */
  override def listHistory: UIO[List[HistHandleFrame]] = histHandleFrameStack.get.map(_.toList)

end SerialSqlExecutorImpl
