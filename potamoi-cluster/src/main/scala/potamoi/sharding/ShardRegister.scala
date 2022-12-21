package potamoi.sharding

import com.devsisters.shardcake.Sharding
import zio.{Scope, UIO, URIO}

/**
 * Mark a service as requiring shardcake sharding entities
 * to be registered in the app startup phase.
 */
trait ShardRegister:
  private[potamoi] def registerEntities: URIO[Sharding with Scope, Unit]
