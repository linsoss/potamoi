package potamoi.flink

import potamoi.flink.FlinkInteractErr.RpcFailure
import potamoi.rpc.Rpc
import zio.{IO, Task}

package object interact:

  extension [Res](rpcIO: Task[Res])
    private[interact] inline def narrowRpcErr: IO[RpcFailure, Res] = Rpc.narrowRpcErr(rpcIO).mapError(RpcFailure.apply)
