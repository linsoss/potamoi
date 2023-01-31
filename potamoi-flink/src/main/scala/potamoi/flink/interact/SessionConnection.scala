package potamoi.flink.interact

import cats.instances.unit
import com.devsisters.shardcake.Messenger
import potamoi.flink.{FlinkConf, FlinkErr, FlinkInteractErr, FlinkMajorVer}
import potamoi.flink.FlinkInterpreterErr.{ExecOperationErr, ExecuteSqlErr, RetrieveResultNothing, SplitSqlScriptErr}
import potamoi.flink.model.interact.*
import potamoi.flink.protocol.FlinkInterpProto
import potamoi.flink.FlinkInteractErr.*
import potamoi.rpc.Rpc
import potamoi.syntax.contra
import potamoi.times.given_Conversion_ScalaDuration_ZIODuration
import potamoi.uuids
import zio.{durationInt, Chunk, Duration, IO, Ref, Schedule, Task, UIO, ZIO}
import zio.ZIO.{fail, succeed}
import zio.stream.{Stream, ZStream}
import zio.ZIOAspect.annotated

/**
 * Flink interactive session connection.
 */
trait SessionConnection:

  def completeSql(sql: String): IO[AttachSessionErr, List[String]]
  def completeSql(sql: String, position: Int): IO[AttachSessionErr, List[String]]

  def submitSqlAsync(sql: String): IO[AttachSessionErr, HandleId]
  def submitSqlScriptAsync(sqlScript: String): IO[FlinkInteractErr, List[ScripSqlSign]]

  def subscribeHandleFrame(handleId: String): HandleFrameWatcher
  def subscribeScriptResultFrame(handleIds: List[String]): ScriptHandleFrameWatcher

  trait HandleFrameWatcher:
    def changing: Stream[AttachHandleErr, HandleFrame]
    def ending: IO[AttachHandleErr, HandleFrame]

  trait ScriptHandleFrameWatcher:
    def changing: Stream[AttachHandleErr, HandleFrame]
    def ending: Stream[AttachHandleErr, HandleFrame]

  def retrieveResultPage(handleId: String, page: Int, pageSize: Int): IO[AttachHandleErr, Option[SqlResultPageView]]
  def retrieveResultOffset(handleId: String, offset: Long, chunkSize: Int): IO[AttachHandleErr, Option[SqlResultOffsetView]]
  def subscribeResultStream(handleId: String): UIO[Stream[FlinkInteractErr, RowValue]]

  def listHandleId: IO[AttachSessionErr, List[HandleId]]
  def listHandleStatus: IO[AttachSessionErr, List[HandleStatusView]]
  def listHandleFrame: IO[AttachSessionErr, List[HandleFrame]]

  def getHandleStatus(handleId: String): IO[AttachHandleErr, HandleStatusView]
  def getHandleFrame(handleId: String): IO[AttachHandleErr, HandleFrame]
  def overview: IO[RpcFailure, SessionOverview]

/**
 * Default implementation.
 */
class SessionConnectionImpl(
    sessionId: String,
    flinkConf: FlinkConf,
    interpreter: Messenger[FlinkInterpProto])
    extends SessionConnection:

  private val streamPollingInterval: Duration            = flinkConf.sqlInteract.streamPollingInterval
  private inline def annoTag[R, E, A](zio: ZIO[R, E, A]) = zio @@ annotated("sessionId" -> sessionId)

  /**
   *  Get completion hints for the given statement.
   */
  override def completeSql(sql: String): IO[AttachSessionErr, List[String]] = annoTag {
    interpreter
      .send(sessionId)(FlinkInterpProto.CompleteSql(sql, sql.length, _))
      .narrowRpcErr
      .flatMap {
        case Left(e: SessionNotYetStarted) => fail(e)
        case Right(v: List[String])        => succeed(v)
      }
  }

  /**
   * Get completion hints for the given statement at the given cursor position.
   */
  override def completeSql(sql: String, position: Int): IO[AttachSessionErr, List[String]] = annoTag {
    interpreter
      .send(sessionId)(FlinkInterpProto.CompleteSql(sql, position, _))
      .narrowRpcErr
      .flatMap {
        case Left(e: SessionNotYetStarted) => fail(e)
        case Right(v: List[String])        => succeed(v)
      }
  }

  /**
   * Submit sql statement.
   */
  override def submitSqlAsync(sql: String): IO[AttachSessionErr, HandleId] = annoTag {
    succeed(uuids.genUUID16).tap(handleId =>
      interpreter
        .send(sessionId)(FlinkInterpProto.SubmitSqlAsync(sql, handleId, _))
        .narrowRpcErr
        .flatMap {
          case Left(e: SessionNotYetStarted) => fail(e)
          case Right(_)                      => ZIO.unit
        })
  }

  /**
   * Submit sql script.
   */
  override def submitSqlScriptAsync(
      sqlScript: String): IO[AttachHandleErr | FailToSplitSqlScript, List[ScripSqlSign]] = annoTag {
    interpreter
      .send(sessionId)(FlinkInterpProto.SubmitSqlScriptAsync(sqlScript, _))
      .narrowRpcErr
      .flatMap {
        case Left(e: SessionNotYetStarted) => fail(e)
        case Left(e: FailToSplitSqlScript) => fail(e)
        case Right(v: List[ScripSqlSign])  => succeed(v)
      }
  }

  /**
   * Receive HandleFrame in stream.
   * Currently using a polling rpc based implementation, the stream rpc implementation
   * needs to wait for the https://github.com/devsisters/shardcake/issues/45
   */
  override def subscribeHandleFrame(handleId: String): HandleFrameWatcher = {
    val stream = ZStream
      .fromZIO(getHandleFrame(handleId))
      .repeat(Schedule.spaced(streamPollingInterval))
      .takeUntil(e => HandleStatuses.isEnd(e.status))

    new HandleFrameWatcher {
      def changing: Stream[AttachHandleErr, HandleFrame] = {
        stream.zipWithPrevious
          .filter { case (prev, cur) => !prev.contains(cur) }
          .map(_._2)
      }
      def ending: IO[AttachHandleErr, HandleFrame]       = {
        stream.runLast.map(_.get)
      }
    }
  }

  /**
   * Receive Sql script's HandleFrame in stream.
   * Currently using a polling rpc based implementation, the stream rpc implementation
   * needs to wait for the https://github.com/devsisters/shardcake/issues/45
   */
  override def subscribeScriptResultFrame(handleIds: List[String]): ScriptHandleFrameWatcher = {
    new ScriptHandleFrameWatcher {
      override def changing: Stream[AttachHandleErr, HandleFrame] = {
        ZStream.concatAll(Chunk.fromIterable(handleIds.map(subscribeHandleFrame(_).changing)))
      }
      override def ending: Stream[AttachHandleErr, HandleFrame]   = {
        ZStream.fromIterable(handleIds).mapZIO(subscribeHandleFrame(_).ending)
      }
    }
  }

  /**
   * Returns sql results in a paged manner.
   *
   * @param page is from 1 on.
   */
  override def retrieveResultPage(handleId: String, page: Int, pageSize: Int): IO[AttachHandleErr, Option[SqlResultPageView]] =
    annoTag {
      interpreter
        .send(sessionId)(FlinkInterpProto.RetrieveResultPage(handleId, page, pageSize, _))
        .narrowRpcErr
        .flatMap {
          case Left(e: SessionNotYetStarted)       => fail(e)
          case Left(e: SessionHandleNotFound)      => fail(e)
          case Right(v: Option[SqlResultPageView]) => succeed(v)
        }
    }

  /**
   * Return sql results as row minimum timestamp offset.
   */
  override def retrieveResultOffset(handleId: String, offset: Long, chunkSize: Int): IO[AttachHandleErr, Option[SqlResultOffsetView]] =
    annoTag {
      interpreter
        .send(sessionId)(FlinkInterpProto.RetrieveResultOffset(handleId, offset, chunkSize, _))
        .narrowRpcErr
        .flatMap {
          case Left(e: SessionNotYetStarted)         => fail(e)
          case Left(e: SessionHandleNotFound)        => fail(e)
          case Right(v: Option[SqlResultOffsetView]) => succeed(v)
        }
    }

  /**
   * Return sql results in stream.
   * Currently using a polling rpc based implementation, the stream rpc implementation
   * needs to wait for the https://github.com/devsisters/shardcake/issues/45
   */
  override def subscribeResultStream(handleId: String): UIO[Stream[AttachHandleErr, RowValue]] = {
    def pollEffect(offsetRef: Ref[Long]): IO[AttachHandleErr, (List[RowValue], Boolean)] =
      for {
        offset   <- offsetRef.get
        sqlRs    <- retrieveResultOffset(handleId, offset, chunkSize = 200)
        rowsFlag <- sqlRs match
                      case None     => getHandleStatus(handleId).map(e => List.empty -> !HandleStatuses.isEnd(e.status))
                      case Some(rs) => offsetRef.set(rs.lastOffset) *> succeed(rs.payload.data -> rs.hasNextRow)
      } yield rowsFlag

    Ref.make[Long](-1).map { offsetRef =>
      ZStream
        .fromZIO(pollEffect(offsetRef))
        .repeat(Schedule.spaced(streamPollingInterval))
        .takeWhile(_._2)
        .map(_._1)
        .flattenIterables
    }
  }

  /**
   * List all handle ids, order by submit time asc.
   */
  override def listHandleId: IO[AttachSessionErr, List[HandleId]] = annoTag {
    interpreter
      .send(sessionId)(FlinkInterpProto.ListHandleId.apply)
      .narrowRpcErr
      .flatMap {
        case Left(e: SessionNotYetStarted) => fail(e)
        case Right(v: List[String])        => succeed(v)
      }
  }

  /**
   * List all handle status, order by submit time asc.
   */
  override def listHandleStatus: IO[AttachSessionErr, List[HandleStatusView]] = annoTag {
    interpreter
      .send(sessionId)(FlinkInterpProto.ListHandleStatus.apply)
      .narrowRpcErr
      .flatMap {
        case Left(e: SessionNotYetStarted)    => fail(e)
        case Right(v: List[HandleStatusView]) => succeed(v)
      }
  }

  /**
   * List all HandleFrame, order by submit time asc.
   */
  override def listHandleFrame: IO[AttachSessionErr, List[HandleFrame]] = annoTag {
    interpreter
      .send(sessionId)(FlinkInterpProto.ListHandleFrame.apply)
      .narrowRpcErr
      .flatMap {
        case Left(e: SessionNotYetStarted) => fail(e)
        case Right(v: List[HandleFrame])   => succeed(v)
      }
  }

  /**
   * Get handle status of given handleId
   */
  override def getHandleStatus(handleId: String): IO[AttachHandleErr, HandleStatusView] =
    annoTag {
      interpreter
        .send(sessionId)(FlinkInterpProto.GetHandleStatus(handleId, _))
        .narrowRpcErr
        .flatMap {
          case Left(e: SessionNotYetStarted)  => fail(e)
          case Left(e: SessionHandleNotFound) => fail(e)
          case Right(v: HandleStatusView)     => succeed(v)
        }
    }

  /**
   * Get HandleFrame of given handle id.
   */
  override def getHandleFrame(handleId: String): IO[AttachHandleErr, HandleFrame] =
    annoTag {
      interpreter
        .send(sessionId)(FlinkInterpProto.GetHandleFrame(handleId, _))
        .narrowRpcErr
        .flatMap {
          case Left(e: SessionNotYetStarted)  => fail(e)
          case Left(e: SessionHandleNotFound) => fail(e)
          case Right(v: HandleFrame)          => succeed(v)
        }
    }

  /**
   * Get session overview info.
   */
  override def overview: IO[RpcFailure, SessionOverview] = annoTag {
    interpreter
      .send(sessionId)(FlinkInterpProto.GetOverview.apply)
      .narrowRpcErr
  }
