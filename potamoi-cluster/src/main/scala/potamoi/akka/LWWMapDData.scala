package potamoi.akka

import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ddata.{LWWMap, LWWMapKey, SelfUniqueAddress}
import akka.cluster.ddata.typed.scaladsl.{DistributedData, Replicator}
import akka.cluster.ddata.Replicator.{GetResponse, UpdateResponse}
import akka.util.Timeout
import potamoi.akka.actors.*
import potamoi.akka.behaviors.*
import potamoi.times.given_Conversion_ScalaDuration_Timeout
import zio.{Duration, IO}

/**
 * Akka LWWMap type DData structure wrapped implementation.
 */
trait LWWMapDData[Key, Value](cacheId: String):

  sealed trait Req

  trait GetReq                                                   extends Req
  final case class Get(key: Key, reply: ActorRef[Option[Value]]) extends GetReq
  final case class Contains(key: Key, reply: ActorRef[Boolean])  extends GetReq
  final case class ListKeys(reply: ActorRef[List[Key]])          extends GetReq
  final case class ListValues(reply: ActorRef[List[Value]])      extends GetReq
  final case class ListAll(reply: ActorRef[Map[Key, Value]])     extends GetReq
  final case class Size(reply: ActorRef[Int])                    extends GetReq

  trait UpdateReq                                                            extends Req
  final case class Put(key: Key, value: Value)                               extends UpdateReq
  final case class Puts(kv: List[(Key, Value)])                              extends UpdateReq
  final case class Remove(key: Key)                                          extends UpdateReq
  final case class Removes(keys: List[Key])                                  extends UpdateReq
  final case class RemoveBySelectKey(filter: Key => Boolean)                 extends UpdateReq
  final case class Update(key: Key, update: Value => Value)                  extends UpdateReq
  final case class Upsert(key: Key, putValue: Value, update: Value => Value) extends UpdateReq

  sealed private trait InternalReq                                                        extends Req
  final private case class InternalUpdate(rsp: UpdateResponse[LWWMap[Key, Value]])        extends InternalReq
  final private case class InternalGet(rsp: GetResponse[LWWMap[Key, Value]], cmd: GetReq) extends InternalReq

  lazy val CacheKey = LWWMapKey[Key, Value](cacheId)

  /**
   * DData actor behavior.
   */
  protected def behavior(conf: DDataConf, init: LWWMap[Key, Value] = LWWMap.empty): Behavior[Req] = Behaviors.setup { implicit ctx =>

    val writeConsistency          = conf.writeConsistency
    val readConsistency           = conf.readConsistency
    given node: SelfUniqueAddress = DistributedData(ctx.system).selfUniqueAddress

    ctx.log.info(s"LWWMap DData [$cacheId] started.")

    DistributedData.withReplicatorMessageAdapter[Req, LWWMap[Key, Value]] { replicator =>

      // noinspection DuplicatedCode
      def updateShape(modify: LWWMap[Key, Value] => LWWMap[Key, Value]) =
        replicator.askUpdate(replyTo => Replicator.Update(CacheKey, init, writeConsistency, replyTo)(modify(_)), rsp => InternalUpdate(rsp))

      Behaviors
        .receiveMessage[Req] {
          case req: GetReq =>
            replicator.askGet(replyTo => Replicator.Get(CacheKey, readConsistency, replyTo), rsp => InternalGet(rsp, req))
            Behaviors.same

          case req: UpdateReq =>
            req match {
              case Put(key, value)           => updateShape(cache => cache.put(node, key, value))
              case Puts(kv)                  => updateShape(cache => kv.foldLeft(cache)((ac, c) => ac.put(node, c._1, c._2)))
              case Remove(key)               => updateShape(cache => cache.remove(node, key))
              case Removes(keys)             => updateShape(cache => keys.foldLeft(cache)((ac, c) => ac.remove(node, c)))
              case RemoveBySelectKey(filter) =>
                updateShape { cache =>
                  cache.entries.keys.filter(filter(_)).foldLeft(cache)((ac, c) => ac.remove(node, c))
                }
              case Update(key, modify)       =>
                updateShape { cache =>
                  cache.get(key) match {
                    case None        => cache
                    case Some(value) => cache.put(node, key, modify(value))
                  }
                }
              case Upsert(key, put, update)  =>
                updateShape { cache =>
                  cache.get(key) match {
                    case None        => cache.put(node, key, put)
                    case Some(value) => cache.put(node, key, update(value))
                  }
                }
            }
            Behaviors.same

          // get replica successfully
          case InternalGet(rsp @ Replicator.GetSuccess(cacheKey), req) =>
            val map = rsp.get(cacheKey)
            req match {
              case Get(key, reply)      => reply ! map.get(key)
              case Contains(key, reply) => reply ! map.contains(key)
              case ListKeys(reply)      => reply ! map.entries.keys.toList
              case ListValues(reply)    => reply ! map.entries.values.toList
              case ListAll(reply)       => reply ! map.entries
              case Size(reply)          => reply ! map.size
            }
            Behaviors.same

          // update replica successfully
          case InternalUpdate(_ @Replicator.UpdateSuccess(_)) =>
            Behaviors.same

          // fail to get replica
          case InternalGet(rsp, req) =>
            rsp match {
              case akka.cluster.ddata.Replicator.NotFound(_, _) =>
                req match
                  case Get(_, reply)      => reply ! None
                  case Contains(_, reply) => reply ! false
                  case ListKeys(reply)    => reply ! List.empty
                  case ListValues(reply)  => reply ! List.empty
                  case ListAll(reply)     => reply ! Map.empty
                  case Size(reply)        => reply ! 0
              case _                                            => ctx.log.error(s"Get data replica failed: ${rsp.toString}")
            }
            Behaviors.same

          // fail to update replica
          case InternalUpdate(rsp) =>
            ctx.log.error(s"Update data replica failed: ${rsp.toString}")
            Behaviors.same
        }
        .onFailure[Exception](SupervisorStrategy.restart)
    }
  }

  /**
   * ZIO interop.
   */
  type AIO[A] = IO[ActorOpErr, A]
  object op:
    extension (actor: ActorRef[Req])(using cradle: ActorCradle) {
      inline def get(key: Key, timeout: Option[Duration] = None): AIO[Option[Value]] = actor.askZIO(Get(key, _), timeout)
      inline def contains(key: Key, timeout: Option[Duration] = None): AIO[Boolean]  = actor.askZIO(Contains(key, _), timeout)
      inline def listKeys(timeout: Option[Duration] = None): AIO[List[Key]]          = actor.askZIO(ListKeys.apply, timeout)
      inline def listValues(timeout: Option[Duration] = None): AIO[List[Value]]      = actor.askZIO(ListValues.apply, timeout)
      inline def listAll(timeout: Option[Duration] = None): AIO[Map[Key, Value]]     = actor.askZIO(ListAll.apply, timeout)
      inline def size(timeout: Option[Duration] = None): AIO[Int]                    = actor.askZIO(Size.apply, timeout)

      inline def put(key: Key, value: Value): AIO[Unit]                                    = actor.tellZIO(Put(key, value))
      inline def puts(kv: List[(Key, Value)]): AIO[Unit]                                   = actor.tellZIO(Puts(kv))
      inline def remove(key: Key): AIO[Unit]                                               = actor.tellZIO(Remove(key))
      inline def removes(keys: List[Key]): AIO[Unit]                                       = actor.tellZIO(Removes(keys))
      inline def removeBySelectKey(filter: Key => Boolean): AIO[Unit]                      = actor.tellZIO(RemoveBySelectKey(filter))
      inline def update(key: Key, updateValue: Value => Value): AIO[Unit]                  = actor.tellZIO(Update(key, updateValue))
      inline def upsert(key: Key, putValue: Value, updateValue: Value => Value): AIO[Unit] = actor.tellZIO(Upsert(key, putValue, updateValue))
    }
