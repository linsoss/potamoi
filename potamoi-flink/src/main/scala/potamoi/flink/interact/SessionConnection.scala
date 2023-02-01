package potamoi.flink.interact

import akka.actor.typed.ActorRef
import cats.instances.unit
import potamoi.{uuids, PotaErr}
import potamoi.akka.{ActorCradle, ActorOpErr}
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
import zio.{durationInt, Chunk, Duration, IO, Ref, Schedule, Task, UIO, ZIO}
import zio.ZIO.{fail, succeed}
import zio.stream.{Stream, ZStream}
import zio.ZIOAspect.annotated

/**
 * Flink interactive session connection.
 */
trait SessionConnection {

  def completeSql(sql: String): IO[AttachSessionErr, List[String]]
  def completeSql(sql: String, position: Int): IO[AttachSessionErr, List[String]]

  def submitSqlAsync(sql: String): IO[AttachSessionErr, HandleId]
  def submitSqlScriptAsync(sqlScript: String): IO[SubmitScriptErr, List[ScripSqlSign]]

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
  def subscribeResultStream(handleId: String): UIO[Stream[AttachHandleErr, RowValue]]

  def listHandleId: IO[AttachSessionErr, List[HandleId]]
  def listHandleStatus: IO[AttachSessionErr, List[HandleStatusView]]
  def listHandleFrame: IO[AttachSessionErr, List[HandleFrame]]
  def getHandleStatus(handleId: String): IO[AttachHandleErr, HandleStatusView]
  def getHandleFrame(handleId: String): IO[AttachHandleErr, HandleFrame]
  def overview: IO[AkkaErr, SessionOverview]
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
  )(using ActorCradle)
    extends SessionConnection {

  private val streamPollingInterval: Duration = flinkConf.sqlInteract.streamPollingInterval

  private inline def annoTag[R, E, A](zio: ZIO[R, E, A]) = zio @@ annotated("sessionId" -> sessionId)

  /**
   * Get completion hints for the given statement.
   */
  override def completeSql(sql: String): IO[AttachSessionErr, List[String]] = annoTag {
    interpreter(sessionId)
      .askZIO(CompleteSql(sql, sql.length, _))
      .mapError(AkkaErr.apply)
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
      .askZIO(CompleteSql(sql, position, _))
      .mapError(AkkaErr.apply)
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
        .askZIO(SubmitSqlAsync(sql, handleId, _))
        .mapError(AkkaErr.apply)
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
      .askZIO(SubmitSqlScriptAsync(sqlScript, _))
      .mapError(AkkaErr.apply)
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

    new HandleFrameWatcher {
      def changing: Stream[AttachHandleErr, HandleFrame] = {
        stream.zipWithPrevious
          .filter { case (prev, cur) => !prev.contains(cur) }
          .map(_._2)
      }

      def ending: IO[AttachHandleErr, HandleFrame] = {
        stream.runLast.map(_.get)
      }
    }
  }

  /**
   * Receive Sql script's HandleFrame in stream.
   * Currently using a polling rpc based implementation.
   * todo replace with actor event subscription implementation.
   */
  override def subscribeScriptResultFrame(handleIds: List[String]): ScriptHandleFrameWatcher = {
    new ScriptHandleFrameWatcher {
      override def changing: Stream[AttachHandleErr, HandleFrame] = {
        ZStream.concatAll(Chunk.fromIterable(handleIds.map(subscribeHandleFrame(_).changing)))
      }

      override def ending: Stream[AttachHandleErr, HandleFrame] = {
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
      interpreter(sessionId)
        .askZIO(RetrieveResultPage(handleId, page, pageSize, _))
        .mapError(AkkaErr.apply)
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
        .askZIO(RetrieveResultOffset(handleId, offset, chunkSize, _))
        .mapError(AkkaErr.apply)
        .flatMap {
          case Left(e: SessionNotYetStarted)         => fail(e)
          case Left(e: SessionHandleNotFound)        => fail(e)
          case Right(v: Option[SqlResultOffsetView]) => succeed(v)
        }
    }

  /**
   * Return sql results in stream.
   * todo replace with actor event subscription implementation.
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
    interpreter(sessionId)
      .askZIO(ListHandleId.apply)
      .mapError(AkkaErr.apply)
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
      .askZIO(ListHandleStatus.apply)
      .mapError(AkkaErr.apply)
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
      .askZIO(ListHandleFrame.apply)
      .mapError(AkkaErr.apply)
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
        .askZIO(GetHandleStatus(handleId, _))
        .mapError(AkkaErr.apply)
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
        .askZIO(GetHandleFrame(handleId, _))
        .mapError(AkkaErr.apply)
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
      .askZIO(Overview.apply)
      .mapError(AkkaErr.apply)
  }
}
