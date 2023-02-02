package potamoi.akka

import akka.actor.typed.*
import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.SupervisorStrategy.restart
import akka.actor.typed.receptionist.{Receptionist, ServiceKey}
import akka.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import akka.cluster.sharding.typed.ClusterShardingSettings.PassivationStrategySettings
import potamoi.akka.actors.*
import potamoi.akka.behaviors.onFailure
import potamoi.syntax.contra
import potamoi.KryoSerializable
import zio.{Duration, IO}

import scala.jdk.CollectionConverters.*
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

  type BindNodeRoleName = String
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
      passivation: Option[PassivationStrategySettings] = None,
      serviceKeyRegister: (Option[ServiceKey[Req]], Option[BindNodeRoleName]) = None -> None): Behavior[Req] = {

    lowLevelBehavior(entityKey, marshallKey, unmarshallKey, serviceKeyRegister) { (ctx, sharding) =>
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
  }

  /**
   * Action behavior.
   */
  protected def lowLevelBehavior(
      entityKey: EntityTypeKey[ProxyCmd],
      marshallKey: ShardKey => String,
      unmarshallKey: String => ShardKey,
      serviceKeyRegister: (Option[ServiceKey[Req]], Option[BindNodeRoleName]) = None -> None
    )(region: (ActorContext[Req], ClusterSharding) => ActorRef[ShardingEnvelope[ProxyCmd]]): Behavior[Req] =
    Behaviors.setup { ctx =>

      val sharding    = ClusterSharding(ctx.system)
      val shardRegion = region(ctx, sharding)
      ctx.log.info(s"Sharding proxy actor for [${entityKey.name}] started.")

      val (serviceKey, bindNodeRole) = (serviceKeyRegister._1, serviceKeyRegister._2)
      val enableServiceRegister      = {
        val curNodeRoles = ctx.system.settings.config.contra { conf =>
          if conf.hasPath("akka.cluster.roles") then conf.getStringList("akka.cluster.roles").asScala else List.empty
        }
        (serviceKey, bindNodeRole) match
          case (None, _)             => false
          case (Some(_), None)       => true
          case (Some(_), Some(role)) => curNodeRoles.contains(role)
      }
      if (enableServiceRegister) ctx.system.receptionist ! Receptionist.Register(serviceKey.get, ctx.self)

      Behaviors
        .receiveMessage[Req] { case Proxy(key, cmd) =>
          shardRegion ! ShardingEnvelope(marshallKey(key), cmd)
          Behaviors.same
        }
        .receiveSignal {
          case (_, PostStop)   =>
            if enableServiceRegister then ctx.system.receptionist ! Receptionist.Deregister(serviceKey.get, ctx.self)
            Behaviors.same
          case (_, PreRestart) =>
            if enableServiceRegister then ctx.system.receptionist ! Receptionist.Register(serviceKey.get, ctx.self)
            Behaviors.same
        }
        .onFailure[Exception](restart)
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
