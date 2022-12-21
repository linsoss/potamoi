package potamoi.sharding

import com.devsisters.shardcake.*
import potamoi.logger.{LogConf, PotaLogger}
import potamoi.zios.asLayer
import zio.{Dequeue, Random, Ref, RIO, Scope, Task, ZIO, ZIOAppArgs, ZIOAppDefault, ZLayer}
import zio.test.ZIOSpecDefault

import scala.util.{Failure, Success, Try}

object ManagerApp extends ZIOAppDefault {
  override val bootstrap = PotaLogger.default

  val run = Server.run.provide(ShardManagerConf().asLayer, ShardManagers.test)
}

object GuildApp extends ZIOAppDefault {
  import GuildBehavior.*
  import GuildBehavior.GuildMessage.*

  override val bootstrap = PotaLogger.default
  val program =
    for {
      _     <- Sharding.registerEntity(Guild, behavior)
      _     <- Sharding.registerScoped
      guild <- Sharding.messenger(Guild)
      user1 <- Random.nextUUID.map(_.toString)
      user2 <- Random.nextUUID.map(_.toString)
      user3 <- Random.nextUUID.map(_.toString)
      _     <- guild.send("guild1")(Join(user1, _)).debug
      _     <- guild.send("guild1")(Join(user2, _)).debug
      _     <- guild.send("guild1")(Join(user3, _)).debug
      _     <- ZIO.never
    } yield ()

  val run = ZIO.scoped(program).provide(ShardingConf().asLayer, Shardings.test)
}

object GuildBehavior {

  sealed trait GuildMessage

  object GuildMessage:
    case class Join(userId: String, replier: Replier[Try[Set[String]]]) extends GuildMessage
    case class Leave(userId: String)                                    extends GuildMessage

  object Guild extends EntityType[GuildMessage]("guild")

  def behavior(entityId: String, messages: Dequeue[GuildMessage]): RIO[Sharding, Nothing] =
    Ref
      .make(Set.empty[String])
      .flatMap(state => messages.take.flatMap(handleMessage(state, _)).forever)

  def handleMessage(state: Ref[Set[String]], message: GuildMessage): RIO[Sharding, Unit] =
    message match
      case GuildMessage.Join(userId, replier) =>
        state.get.flatMap { members =>
          if (members.size >= 5)
            replier.reply(Failure(Exception("Guild is already full!")))
          else
            state.updateAndGet(_ + userId).flatMap { newMembers =>
              replier.reply(Success(newMembers))
            }
        }
      case GuildMessage.Leave(userId) =>
        state.update(_ - userId)
}
