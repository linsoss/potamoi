package potamoi.akka

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.SupervisorStrategy.restart
import akka.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import akka.cluster.sharding.typed.ClusterShardingSettings.PassivationStrategySettings
import potamoi.akka.actors.*
import potamoi.akka.behaviors.onFailure
import potamoi.syntax.contra
import zio.{Duration, IO}

import scala.reflect.ClassTag

/**
 * Cluster sharding proxy for Akka actor.
 *
 * @tparam ShardKey type of sharding key
 * @tparam ProxyCmd type of proxy actor
 */
trait ShardingProxy[ShardKey, ProxyCmd]:

  sealed trait Req                                     extends KryoSerializable
  final case class Proxy(key: ShardKey, cmd: ProxyCmd) extends Req

  /**
   * Action behavior.
   */
  protected def behavior(
      entityKey: EntityTypeKey[ProxyCmd],
      marshallKey: ShardKey => String,
      unmarshallKey: String => ShardKey
    )(region: (ActorContext[Req], ClusterSharding) => ActorRef[ShardingEnvelope[ProxyCmd]]): Behavior[Req] =
    Behaviors.setup { ctx =>
      val sharding    = ClusterSharding(ctx.system)
      val shardRegion = region(ctx, sharding)
      ctx.log.info(s"Sharding proxy actor for [${entityKey.name}] started.")
      Behaviors
        .receiveMessage[Req] { case Proxy(key, cmd) =>
          shardRegion ! ShardingEnvelope(marshallKey(key), cmd)
          Behaviors.same
        }
        .onFailure[Exception](restart)
    }

  /**
   * Simpler action behavior.
   */
  protected def behavior(
      entityKey: EntityTypeKey[ProxyCmd],
      marshallKey: ShardKey => String,
      unmarshallKey: String => ShardKey,
      createBehavior: ShardKey => Behavior[ProxyCmd],
      stopMessage: Option[ProxyCmd] = None,
      bindRole: Option[String] = None,
      passivation: Option[PassivationStrategySettings] = None): Behavior[Req] = behavior(entityKey, marshallKey, unmarshallKey) { (ctx, sharding) =>
    sharding.init {
      Entity(entityKey)(entityCtx => createBehavior(unmarshallKey(entityCtx.entityId)))
        .contra { it =>
          stopMessage match
            case Some(message) => it.withStopMessage(message)
            case None          => it
        }
        .contra { it =>
          bindRole match
            case Some(role) => it.withRole(role)
            case None       => it
        }
        .contra { it =>
          passivation match
            case Some(settings) => it.withSettings(ClusterShardingSettings(ctx.system).withPassivationStrategy(settings))
            case None           => it.withSettings(ClusterShardingSettings(ctx.system).withNoPassivationStrategy()) // not passivate shards by default
        }
    }
  }

  /**
   * ZIO interop.
   */
  type AIO[A] = IO[ActorOpErr, A]

  implicit class ops(actor: ActorRef[Req])(implicit cradle: ActorCradle) {

    def apply(key: ShardKey): ProxyPartiallyApplied = ProxyPartiallyApplied(key)

    case class ProxyPartiallyApplied(key: ShardKey):
      inline def tellZIO(cmd: ProxyCmd): AIO[Unit] = actor.tellZIO(Proxy(key, cmd))

      inline def askZIO[Res: ClassTag](cmd: ActorRef[Res] => ProxyCmd, timeout: Option[Duration] = None): AIO[Res] =
        actor.askZIO[Res](ref => Proxy(key, cmd(ref)), timeout)
  }
