package potamoi.rpc

import com.devsisters.shardcake.{EntityType, Replier, Sharding}
import potamoi.logger.PotaLogger
import zio.{Console, UIO, URIO, ZIO, ZIOAppDefault}
import potamoi.sharding.LocalShardManager.withLocalShardManager
import potamoi.sharding.{ShardingConf, Shardings}
import potamoi.uuids
import potamoi.zios.asLayer

/**
 * Proto
 */
sealed trait BotProto
object BotEntity extends EntityType[BotProto]("test")

object BotProto:
  case class Echo(msg: String, replier: Replier[String]) extends BotProto
  case class Touch(msg: String)                          extends BotProto

/**
 * Server
 */
object BotRpcService extends RpcService[BotProto](BotEntity):
  def handleMessage(message: BotProto): URIO[Sharding, Unit] = message match {
    case BotProto.Echo(msg, replier) => replier.reply("echo: " + msg)
    case BotProto.Touch(msg)         => Console.printLine(s"be touch: $msg").ignore
  }

object BotRpcServiceApp extends ZIOAppDefault:
  val run = ZIO
    .scoped {
      BotRpcService.registerEntities *>
      Sharding.registerScoped *>
      ZIO.never
    }
    .provide(ShardingConf.test.copy(selfPort = 54323).asLayer, Shardings.test)
    .withLocalShardManager
    .provideLayer(PotaLogger.default)

/**
 * Client
 */
object BotRpcClientApp extends ZIOAppDefault:
  val effect =
    for {
      bot       <- Rpc(BotEntity)
      echoReply <- bot.ask(BotProto.Echo("hello", _))
      _         <- bot.tell(BotProto.Touch("hi"))
      _         <- Console.printLine(echoReply)
    } yield ()

  val run = effect.provide(ShardingConf.test.copy(selfPort = 54322).asLayer, Shardings.test)
