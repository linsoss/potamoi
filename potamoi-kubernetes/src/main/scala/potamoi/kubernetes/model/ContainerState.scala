package potamoi.kubernetes.model

import com.coralogix.zio.k8s.model.core.v1.ContainerState as RawContainerState
import potamoi.kubernetes.*
import zio.json.{DeriveJsonCodec, JsonCodec, JsonDecoder, JsonEncoder, jsonField}
import zio.prelude.data.Optional.{Absent, Present}

import scala.util.Try

/**
 * k8s container state.
 */
enum ContainerState:
  case Running, Terminated, Waiting, Unknown

object ContainerStates:
  given JsonCodec[ContainerState] = JsonCodec(
    JsonEncoder[String].contramap(_.toString),
    JsonDecoder[String].map(s => Try(ContainerState.valueOf(s)).getOrElse(ContainerState.Unknown))
  )

sealed trait ContainerStateDetail

@jsonField("running") case class ContainerRunning(
    startedAt: Option[Long])
    extends ContainerStateDetail

@jsonField("terminated") case class ContainerTerminated(
    exitCode: Int,
    message: Option[String],
    reason: Option[String],
    signal: Option[Int],
    startedAt: Option[Long],
    finishedAt: Option[Long])
    extends ContainerStateDetail

@jsonField("waiting") case class ContainerWaiting(
    message: Option[String],
    reason: Option[String])
    extends ContainerStateDetail

@jsonField("unknown") case object ContainerStateUnknown extends ContainerStateDetail

object ContainerStateDetail:
  given JsonCodec[ContainerStateDetail] = DeriveJsonCodec.gen[ContainerStateDetail]

  /**
   * resolve raw [[com.coralogix.zio.k8s.model.core.v1.ContainerState]]
   */
  def resolve(state: RawContainerState): (ContainerState, ContainerStateDetail) = {
    state.running match {
      case Present(s) => ContainerState.Running -> ContainerRunning(s.startedAt.map(_.value.toEpochSecond).toOption)
      case Absent =>
        state.waiting match {
          case Present(s) => ContainerState.Waiting -> ContainerWaiting(s.message.toOption, s.reason.toOption)
          case Absent =>
            state.terminated match {
              case Present(s) =>
                ContainerState.Terminated -> ContainerTerminated(
                  s.exitCode,
                  s.message.toOption,
                  s.reason.toOption,
                  s.signal.toOption,
                  s.startedAt.map(_.value.toEpochSecond).toOption,
                  s.finishedAt.map(_.value.toEpochSecond).toOption)
              case Absent => ContainerState.Unknown -> ContainerStateUnknown
            }
        }
    }
  }
