package potamoi.rpc

import com.devsisters.shardcake.{EntityType, Replier, Sharding}
import potamoi.logger.PotaLogger
import potamoi.sharding.{ShardingConf, Shardings}
import potamoi.sharding.LocalShardManager.withLocalShardManager
import potamoi.uuids
import potamoi.zios.asLayer
import zio.{Console, UIO, URIO, ZIO, ZIOAppDefault}

/**
 * Proto
 */
sealed trait BotProto
object BotEntity extends EntityType[BotProto]("test")

object BotProto:
  case class Echo(msg: String, replier: Replier[String])                                extends BotProto
  case class Touch(msg: String)                                                         extends BotProto
  case class UnionType(sign: Int, replier: Replier[Int | String])                       extends BotProto // union type serialization test
  case class UnionTypeEither(sign: Int, replier: Replier[Either[Int | String, String]]) extends BotProto // union type serialization test

/**
 * Server
 */
object BotRpcService extends RpcService[BotProto](BotEntity):
  def handleMessage(message: BotProto): URIO[Sharding, Unit] = message match {
    case BotProto.Echo(msg, replier)             => replier.reply("echo: " + msg)
    case BotProto.Touch(msg)                     => Console.printLine(s"be touch: $msg").ignore
    case BotProto.UnionType(sign, replier)       => if sign > 10 then replier.reply(2333) else replier.reply("boom")
    case BotProto.UnionTypeEither(sign, replier) =>
      if sign < 0 then replier.reply(Right("pass"))
      else if sign < 10 then replier.reply(Left(2333))
      else replier.reply(Left("boom"))
  }

object BotRpcServiceApp extends ZIOAppDefault:
  val run = ZIO
    .scoped {
      BotRpcService.registerEntities *>
      Sharding.registerScoped *>
      ZIO.never
    }
    .provide(ShardingConf.test.project(_.copy(selfPort = 54323)), Shardings.test)
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
      _         <- Console.printLine(echoReply)
      _         <- bot.tell(BotProto.Touch("hi"))
      _         <- bot.ask(BotProto.UnionType(1, _)).debug
      _         <- bot.ask(BotProto.UnionTypeEither(-2, _)).debug
      _         <- bot.ask(BotProto.UnionTypeEither(5, _)).debug
      _         <- bot.ask(BotProto.UnionTypeEither(10, _)).debug
    } yield ()

  val run = effect.provide(ShardingConf.test.project(_.copy(selfPort = 54322)), Shardings.test)
