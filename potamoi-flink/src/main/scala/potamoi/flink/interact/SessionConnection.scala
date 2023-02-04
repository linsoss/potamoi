package potamoi.flink.interact

import akka.actor.typed.ActorRef
import cats.instances.unit
import potamoi.{uuids, PotaErr}
import potamoi.akka.*
import potamoi.flink.{FlinkConf, FlinkErr, FlinkInteractErr, FlinkMajorVer}
import potamoi.flink.interact.SessionConnection.*
import potamoi.flink.interpreter.FlinkInterpreter
import potamoi.flink.model.interact.*
import potamoi.flink.FlinkInteractErr.{FailToSplitSqlScript, SessionHandleNotFound, SessionNotYetStarted}
import potamoi.flink.interpreter.FlinkInterpreter.ops
import potamoi.flink.interpreter.FlinkInterpreterActor.*
import potamoi.flink.FlinkErr.AkkaErr
import potamoi.syntax.contra
import potamoi.times.given_Conversion_ScalaDuration_ZIODuration
import zio.{durationInt, Chunk, Duration, IO, Ref, Schedule, Task, UIO, ZIO, ZIOAppDefault}
import zio.ZIO.{fail, logInfo, succeed}
import zio.stream.{Stream, ZStream}
import zio.Console.printLine
import zio.ZIOAspect.annotated

/**
 * Flink interactive session connection.
 */
trait SessionConnection {

  /**
   * Get session overview info.
   */
  def overview: IO[AkkaErr, SessionOverview]

  /**
   * Get completion hints for the given statement at the given cursor position.
   */
  def completeSql(sql: String, position: Int): IO[AttachSessionErr, List[String]]

  /**
   * Submit sql statement.
   */
  def submitSqlAsync(sql: String): IO[AttachSessionErr, HandleId]

  /**
   * Submit sql script.
   */
  def submitSqlScriptAsync(sqlScript: String): IO[SubmitScriptErr, List[ScripSqlSign]]

  /**
   * Subscribe to handle frame information by given handle id.
   */
  def subscribeHandleFrame(handleId: String): HandleFrameWatcher

  trait HandleFrameWatcher:
    def changing: Stream[AttachHandleErr, HandleFrame]
    def ending: IO[AttachHandleErr, HandleFrame]
    def hybridChanging: Stream[AttachHandleErr, (HandleFrame, Option[Stream[AttachHandleErr, RowValue]])]

  /**
   * Subscribe to script result frame information by given handle ids.
   */
  def subscribeScriptResultFrame(handleIds: List[String]): ScriptHandleFrameWatcher

  trait ScriptHandleFrameWatcher:
    def changing: Stream[AttachHandleErr, HandleFrame]
    def ending: Stream[AttachHandleErr, HandleFrame]
    def hybridChanging: Stream[AttachHandleErr, (HandleFrame, Option[Stream[AttachHandleErr, RowValue]])]

  /**
   * Returns sql results in a paged manner.
   * @param page is from 1 on.
   */
  def retrieveResultPage(handleId: String, page: Int, pageSize: Int): IO[AttachHandleErr, Option[SqlResultPageView]]

  /**
   * Return sql results as row minimum timestamp offset.
   */
  def retrieveResultOffset(handleId: String, offset: Long, chunkSize: Int): IO[AttachHandleErr, Option[SqlResultOffsetView]]

  /**
   * Return sql results as a stream.
   */
  def subscribeResultStream(handleId: String): Stream[AttachHandleErr, RowValue]

  /**
   * Get completion hints for the given statement.
   */
  def completeSql(sql: String): IO[AttachSessionErr, List[String]]

  /**
   * List all handle ids, order by submit time asc.
   */
  def listHandleId: IO[AttachSessionErr, List[HandleId]]

  /**
   * List all handle status, order by submit time asc.
   */
  def listHandleStatus: IO[AttachSessionErr, List[HandleStatusView]]

  /**
   * List all HandleFrame, order by submit time asc.
   */
  def listHandleFrame: IO[AttachSessionErr, List[HandleFrame]]

  /**
   * Get handle status of given handleId
   */
  def getHandleStatus(handleId: String): IO[AttachHandleErr, HandleStatusView]

  /**
   * Get HandleFrame of given handle id.
   */
  def getHandleFrame(handleId: String): IO[AttachHandleErr, HandleFrame]
}

object SessionConnection {
  type AttachSessionErr = (SessionNotYetStarted | AkkaErr) with PotaErr
  type AttachHandleErr  = (SessionNotYetStarted | SessionHandleNotFound | AkkaErr) with PotaErr
  type SubmitScriptErr  = (AttachHandleErr | FailToSplitSqlScript) with PotaErr
}

/**
 * Default implementation.
 */
class SessionConnectionImpl(
    sessionId: String,
    flinkConf: FlinkConf,
    interpreter: ActorRef[FlinkInterpreter.Req]
  )(using AkkaMatrix)
    extends SessionConnection {

  private val streamPollingInterval: Duration = flinkConf.sqlInteract.streamPollingInterval

  private inline def annoTag[R, E, A](zio: ZIO[R, E, A]) = zio @@ annotated("sessionId" -> sessionId)

  /**
   * Get completion hints for the given statement.
   */
  override def completeSql(sql: String): IO[AttachSessionErr, List[String]] = annoTag {
    interpreter(sessionId)
      .askZIO[EitherPack[SessionNotYetStarted, List[String]]](CompleteSql(sql, sql.length, _))
      .mapBoth(AkkaErr.apply, _.value)
      .flatMap {
        case Left(e: SessionNotYetStarted) => fail(e)
        case Right(v: List[String])        => succeed(v)
      }
  }

  /**
   * Get completion hints for the given statement at the given cursor position.
   */
  override def completeSql(sql: String, position: Int): IO[AttachSessionErr, List[String]] = annoTag {
    interpreter(sessionId)
      .askZIO[EitherPack[SessionNotYetStarted, List[String]]](CompleteSql(sql, position, _))
      .mapBoth(AkkaErr.apply, _.value)
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
      interpreter(sessionId)
        .askZIO[EitherPack[SessionNotYetStarted, Ack.type]](SubmitSqlAsync(sql, handleId, _))
        .mapBoth(AkkaErr.apply, _.value)
        .flatMap {
          case Left(e: SessionNotYetStarted) => fail(e)
          case Right(_)                      => ZIO.unit
        })
  }

  /**
   * Submit sql script.
   */
  override def submitSqlScriptAsync(
      sqlScript: String): IO[SubmitScriptErr, List[ScripSqlSign]] = annoTag {
    interpreter(sessionId)
      .askZIO[EitherPack[InternalSubmitScriptErr, List[ScripSqlSign]]](SubmitSqlScriptAsync(sqlScript, _))
      .mapBoth(AkkaErr.apply, _.value)
      .flatMap {
        case Left(e: SessionNotYetStarted) => fail(e)
        case Left(e: FailToSplitSqlScript) => fail(e)
        case Right(v: List[ScripSqlSign])  => succeed(v)
      }
  }

  /**
   * Receive HandleFrame in stream.
   * todo replace with actor event subscription implementation.
   */
  override def subscribeHandleFrame(handleId: String): HandleFrameWatcher = {

    val stream = ZStream
      .fromZIO(getHandleFrame(handleId))
      .repeat(Schedule.spaced(streamPollingInterval))
      .takeUntil(e => HandleStatuses.isEnd(e.status))

    new HandleFrameWatcher:
      import HandleStatus.*

      def changing: Stream[AttachHandleErr, HandleFrame] = stream.changes
      def ending: IO[AttachHandleErr, HandleFrame]       = stream.runLast.map(_.get)

      def hybridChanging: Stream[AttachHandleErr, (HandleFrame, Option[Stream[AttachHandleErr, RowValue]])] =
        stream.changes.mapZIO { frame =>
          (frame.status, frame.result) match
            case (Run | Finish | Cancel, Some(_: QuerySqlRsDescriptor)) => succeed(frame, Some(subscribeResultStream(handleId)))
            case _                                                      => succeed(frame, None)
        }
  }

  /**
   * Receive Sql script's HandleFrame in stream.
   * Currently using a polling rpc based implementation.
   * todo replace with actor event subscription implementation.
   */
  override def subscribeScriptResultFrame(handleIds: List[String]): ScriptHandleFrameWatcher = {
    new ScriptHandleFrameWatcher:

      def changing: Stream[AttachHandleErr, HandleFrame] =
        ZStream.concatAll(Chunk.fromIterable(handleIds.map(subscribeHandleFrame(_).changing)))

      def ending: Stream[AttachHandleErr, HandleFrame] =
        ZStream.fromIterable(handleIds).mapZIO(subscribeHandleFrame(_).ending)

      def hybridChanging: Stream[AttachHandleErr, (HandleFrame, Option[Stream[AttachHandleErr, RowValue]])] =
        ZStream.concatAll(Chunk.fromIterable(handleIds.map(subscribeHandleFrame(_).hybridChanging)))

  }

  /**
   * Returns sql results in a paged manner.
   */
  override def retrieveResultPage(handleId: String, page: Int, pageSize: Int): IO[AttachHandleErr, Option[SqlResultPageView]] =
    annoTag {
      interpreter(sessionId)
        .askZIO[EitherPack[InternalAttachHandleErr, Option[SqlResultPageView]]](RetrieveResultPage(handleId, page, pageSize, _))
        .mapBoth(AkkaErr.apply, _.value)
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
      interpreter(sessionId)
        .askZIO[EitherPack[InternalAttachHandleErr, Option[SqlResultOffsetView]]](RetrieveResultOffset(handleId, offset, chunkSize, _))
        .mapBoth(AkkaErr.apply, _.value)
        .flatMap {
          case Left(e: SessionNotYetStarted)         => fail(e)
          case Left(e: SessionHandleNotFound)        => fail(e)
          case Right(v: Option[SqlResultOffsetView]) => succeed(v)
        }
    }

  /**
   * Return sql results as a stream.
   * todo replace with actor event subscription implementation.
   */
  override def subscribeResultStream(handleId: String): Stream[AttachHandleErr, RowValue] = {
    for {
      offsetRef <- ZStream.from(Ref.make[Long](-1))
      isEndRef  <- ZStream.from(Ref.make[Boolean](false))
      // single pull data effect
      pullData   = for {
                     offset    <- offsetRef.get
                     sqlResult <- retrieveResultOffset(handleId, offset, chunkSize = 200)
                     dataRows  <- sqlResult match
                                    case Some(data) =>
                                      offsetRef.set(data.lastOffset) *>
                                      isEndRef.set(!data.hasNextRow) *>
                                      succeed(data.payload.data)
                                    case None       =>
                                      getHandleStatus(handleId).flatMap(s => isEndRef.set(HandleStatuses.isEnd(s.status))) *>
                                      succeed(List.empty)
                   } yield dataRows
      // pull data stream
      row       <- ZStream
                     .fromZIO(pullData)
                     .repeat(Schedule.spaced(streamPollingInterval))
                     .takeUntilZIO(_ => isEndRef.get)
                     .flattenIterables
    } yield row
  }

  /**
   * List all handle ids, order by submit time asc.
   */
  override def listHandleId: IO[AttachSessionErr, List[HandleId]] = annoTag {
    interpreter(sessionId)
      .askZIO[EitherPack[SessionNotYetStarted, List[String]]](ListHandleId.apply)
      .mapBoth(AkkaErr.apply, _.value)
      .flatMap {
        case Left(e: SessionNotYetStarted) => fail(e)
        case Right(v: List[String])        => succeed(v)
      }
  }

  /**
   * List all handle status, order by submit time asc.
   */
  override def listHandleStatus: IO[AttachSessionErr, List[HandleStatusView]] = annoTag {
    interpreter(sessionId)
      .askZIO[EitherPack[SessionNotYetStarted, List[HandleStatusView]]](ListHandleStatus.apply)
      .mapBoth(AkkaErr.apply, _.value)
      .flatMap {
        case Left(e: SessionNotYetStarted)    => fail(e)
        case Right(v: List[HandleStatusView]) => succeed(v)
      }
  }

  /**
   * List all HandleFrame, order by submit time asc.
   */
  override def listHandleFrame: IO[AttachSessionErr, List[HandleFrame]] = annoTag {
    interpreter(sessionId)
      .askZIO[EitherPack[SessionNotYetStarted, List[HandleFrame]]](ListHandleFrame.apply)
      .mapBoth(AkkaErr.apply, _.value)
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
      interpreter(sessionId)
        .askZIO[EitherPack[InternalAttachHandleErr, HandleStatusView]](GetHandleStatus(handleId, _))
        .mapBoth(AkkaErr.apply, _.value)
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
      interpreter(sessionId)
        .askZIO[EitherPack[InternalAttachHandleErr, HandleFrame]](GetHandleFrame(handleId, _))
        .mapBoth(AkkaErr.apply, _.value)
        .flatMap {
          case Left(e: SessionNotYetStarted)  => fail(e)
          case Left(e: SessionHandleNotFound) => fail(e)
          case Right(v: HandleFrame)          => succeed(v)
        }
    }

  /**
   * Get session overview info.
   */
  override def overview: IO[AkkaErr, SessionOverview] = annoTag {
    interpreter(sessionId)
      .askZIO[ValuePack[SessionOverview]](Overview.apply)
      .mapBoth(AkkaErr.apply, _.value)
  }
}
