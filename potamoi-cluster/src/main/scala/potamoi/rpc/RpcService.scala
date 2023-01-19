package potamoi.rpc

import com.devsisters.shardcake.{EntityType, Messenger, Replier, Sharding}
import potamoi.sharding.ShardRegister
import zio.{Dequeue, Duration, RIO, Scope, Tag, UIO, URIO}

/**
 * Rpc service based on shardcake sharding.
 */
abstract class RpcService[R: Tag](entity: EntityType[R]) extends ShardRegister:

  protected def handleMessage(message: R): URIO[Sharding, Unit]

  private def behavior(entityId: String, messages: Dequeue[R]): RIO[Sharding with Scope, Nothing] = {
    messages.take.flatMap { msg => handleMessage(msg).forkScoped }.forever
  }

  override private[potamoi] def registerEntities: URIO[Sharding with Scope, Unit] = {
    Sharding.registerEntity(entity, behavior)
  }
