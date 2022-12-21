package potamoi.flink.observer

import potamoi.common.Syntax.toPrettyString
import potamoi.flink.model.Fcid
import potamoi.syntax.contra
import zio.{Duration, IO, Ref, Schedule, UIO}
import zio.ZIO.logError
import zio.direct.*

package object tracker:

  /**
   * Marshall Fcid as shardcake entity-id.
   */
  def marshallFcid(fcid: Fcid): String = s"${fcid.clusterId}@${fcid.namespace}"

  /**
   * Unmarshall Fcid from shardcake entity-id.
   */
  def unmarshallFcid(str: String): Fcid = str.split('@').contra(arr => arr(0) -> arr(1))

  /**
   * Cyclic trigger polling effect and recording of the first non-repeating error.
   */
  inline def loopTrigger[E, A](spaced: Duration, effect: IO[E, A]): UIO[Unit] =
    for {
      preErr <- Ref.make[Option[E]](None)
      loopEffect <- effect
        .tapError { err =>
          preErr.get.flatMap { pre =>
            (logError(toPrettyString(err)) *> preErr.set(Some(err))).when(!pre.contains(err))
          }
        }
        .ignore
        .schedule(Schedule.spaced(spaced))
        .forever
    } yield loopEffect

  case object Ack