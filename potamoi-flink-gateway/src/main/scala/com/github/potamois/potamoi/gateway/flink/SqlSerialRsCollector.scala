package com.github.potamois.potamoi.gateway.flink

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import com.github.potamois.potamoi.commons.{CborSerializable, FiniteQueue, curTs}

import scala.collection.mutable.ArrayBuffer


/**
 * @author Al-assad
 */
trait TraceableExecRs extends CborSerializable

case class ImmediateOpDone(data: TableResultData) extends TraceableExecRs

case class SubmitModifyOpDone(collector: ActorRef[ModifyOpRsCollector.Command]) extends TraceableExecRs

case class SubmitQueryOpDone(collector: ActorRef[QueryOpRsCollector.Command]) extends TraceableExecRs


/**
 * Flink TableResult collector actor for Modify Operations.
 *
 * @author Al-assad
 */
object ModifyOpRsCollector {
  trait Command
  final case class GetResult(replyTo: ActorRef[Either[TsError, Result]]) extends Command
  final case class IsFinished(replyTo: ActorRef[Boolean]) extends Command

  private[flink] final case class EmitResultData(data: TableResultData) extends Command
  private[flink] final case class EmitJobId(jobId: String) extends Command
  private[flink] final case class EmitError(error: Error) extends Command

  case class Result(jobId: String, data: Option[TableResultData], isFinished: Boolean, ts: Long) extends Command

  def apply(sessionId: String): Behavior[Command] = Behaviors.setup { ctx =>
    var jobId = ""
    var data: Option[TableResultData] = None
    var error: Option[TsError] = None
    var isFinished = false
    var ts = curTs

    Behaviors.receiveMessage {
      case EmitJobId(_jobId) =>
        jobId = _jobId
        ts = curTs
        Behaviors.same
      case EmitError(_error) =>
        error = Some(_error.toTsError(ts))
        isFinished = true
        ts = curTs
        Behaviors.same
      case EmitResultData(_data) =>
        data = Some(_data)
        isFinished = true
        ts = curTs
        Behaviors.same

      case GetResult(replyTo) =>
        if (error.isDefined) replyTo ! Left(error.get)
        else replyTo ! Right(Result(jobId, data, isFinished, ts))
        Behaviors.same
      case IsFinished(replyTo) =>
        replyTo ! isFinished
        Behaviors.same
    }
  }
}

/**
 * Flink TableResult collector actor for Query Operations
 *
 * @author Al-assad
 */
object QueryOpRsCollector {
  trait Command

  // todo GetAll, Pageable Query Result, and Other
  final case class GetResult(replyTo: ActorRef[Either[TsError, Result]]) extends Command
  final case class IsFinished(replyTo: ActorRef[Boolean]) extends Command

  private[flink] final case class EmitJobId(jobId: String) extends Command
  private[flink] final case class EmitError(error: Error) extends Command
  private[flink] final case class EmitResultColumns(cols: Seq[Column]) extends Command
  private[flink] final case class EmitResultRow(row: RowData) extends Command
  private[flink] final case object EmitCollectRowsDone extends Command

  case class Result(jobId: String, data: Option[TableResultData], isFinished: Boolean, ts: Long) extends Command

  def apply(sessionId: String,
            collectStrategy: RsCollectStrategy): Behavior[Command] = Behaviors.setup { ctx =>
    var jobId = ""
    var error: Option[TsError] = None
    var isFinished = false
    var columns = Seq.empty[Column]
    val rows = collectStrategy match {
      case RsCollectStrategy(EvictStrategy.DROP_HEAD, limit) => FiniteQueue[RowData](limit)
      case RsCollectStrategy(EvictStrategy.DROP_TAIL, limit) => new ArrayBuffer[RowData](limit + 10)
    }
    var ts = curTs

    Behaviors.receiveMessage {
      case EmitJobId(_jobId) =>
        jobId = _jobId
        ts = curTs
        Behaviors.same
      case EmitError(_error) =>
        error = Some(_error.toTsError(ts))
        isFinished = true
        ts = curTs
        Behaviors.same
      case EmitResultColumns(cols) =>
        columns = cols
        ts = curTs
        Behaviors.same
      case EmitResultRow(row) =>
        rows += row
        ts = curTs
        Behaviors.same

      case EmitCollectRowsDone =>
        isFinished = true
        ts = curTs
        Behaviors.same

      case IsFinished(replyTo) =>
        replyTo ! isFinished
        Behaviors.same

      // todo
      case GetResult(replyTo) =>
        if (error.isDefined) replyTo ! Left(error.get)
        else replyTo ! Right(Result(jobId, Some(TableResultData(columns, rows)), isFinished, ts))
        Behaviors.same
    }
  }
}


