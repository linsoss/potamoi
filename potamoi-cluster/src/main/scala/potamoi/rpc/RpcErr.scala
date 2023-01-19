package potamoi.rpc

import com.devsisters.shardcake.errors.SendTimeoutException
import potamoi.PotaErr

sealed trait RpcErr extends PotaErr

object RpcErr:
  case class CallRpcFailure(cause: Throwable)               extends RpcErr
  case class CallRpcTimeout(cause: SendTimeoutException[_]) extends RpcErr
