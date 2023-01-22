package potamoi.rpc

import com.devsisters.shardcake.{EntityType, Messenger, Replier, Sharding}
import com.devsisters.shardcake.errors.SendTimeoutException
import zio.{Duration, IO, Random, Tag, Task, UIO, URIO, ZIO}

/**
 * Rpc client based on shardcake sharding.
 */
class RpcClient[Msg](messenger: Messenger[Msg], retry: Option[Int]):

  def ask[Res](msg: Replier[Res] => Msg): IO[RpcErr, Res] = {
    Random.nextUUID
      .flatMap(uuid => messenger.send(uuid.toString)(msg))
      .mapError {
        case err: SendTimeoutException[_] => RpcErr.CallRpcTimeout(err)
        case err                          => RpcErr.CallRpcFailure(err)
      }
      .retryN(retry.getOrElse(1))
  }

  def tell(msg: Msg): UIO[Unit] = {
    Random.nextUUID.flatMap(uuid => messenger.sendDiscard(uuid.toString)(msg))
  }

object Rpc:

  def apply[Msg: Tag](
      entityType: EntityType[Msg],
      callTimeout: Option[Duration] = None,
      retry: Option[Int] = None): URIO[Sharding, RpcClient[Msg]] = {
    Sharding.messenger(entityType, callTimeout).map(messenger => RpcClient(messenger, retry))
  }

  def narrowRpcErr[Res](sendMsgIO: Task[Res]): IO[RpcErr, Res] = sendMsgIO.mapError {
    case err: SendTimeoutException[_] => RpcErr.CallRpcTimeout(err)
    case err                          => RpcErr.CallRpcFailure(err)
  }
