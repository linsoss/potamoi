package potamoi.akka

import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.ddata.{ORSet, ORSetKey, SelfUniqueAddress}
import akka.cluster.ddata.Replicator.{GetResponse, UpdateResponse}
import akka.cluster.ddata.typed.scaladsl.{DistributedData, Replicator}
import potamoi.akka.actors.*
import potamoi.akka.behaviors.*
import zio.{Duration, IO}

/**
 * Akka ORSet type DData structure wrapped implementation.
 */
trait ORSetDData[Value](cacheId: String):

  sealed trait Req

  trait GetReq                                                      extends Req
  final case class List(reply: ActorRef[Set[Value]])                extends GetReq
  final case class Contains(value: Value, reply: ActorRef[Boolean]) extends GetReq
  final case class Size(reply: ActorRef[Int])                       extends GetReq

  trait UpdateReq                              extends Req
  final case class Put(value: Value)           extends UpdateReq
  final case class Puts(values: Set[Value])    extends UpdateReq
  final case class Remove(value: Value)        extends UpdateReq
  final case class Removes(values: Set[Value]) extends UpdateReq
  final case object Clear                      extends UpdateReq

  sealed private trait InternalReq                                                  extends Req
  final private case class InternalUpdate(rsp: UpdateResponse[ORSet[Value]])        extends InternalReq
  final private case class InternalGet(rsp: GetResponse[ORSet[Value]], cmd: GetReq) extends InternalReq

  lazy val CacheKey = ORSetKey[Value](cacheId)

  /**
   * DData actor behavior.
   */
  protected def behavior(conf: DDataConf, init: ORSet[Value] = ORSet.empty): Behavior[Req] = Behaviors.setup { implicit ctx =>

    val writeConsistency          = conf.writeConsistency
    val readConsistency           = conf.readConsistency
    given node: SelfUniqueAddress = DistributedData(ctx.system).selfUniqueAddress

    ctx.log.info(s"ORSet DData [$cacheId] started.")

    DistributedData.withReplicatorMessageAdapter[Req, ORSet[Value]] { replicator =>

      // noinspection DuplicatedCode
      def updateShape(modify: ORSet[Value] => ORSet[Value]) =
        replicator.askUpdate(replyTo => Replicator.Update(CacheKey, init, writeConsistency, replyTo)(modify(_)), rsp => InternalUpdate(rsp))

      Behaviors
        .receiveMessage[Req] {
          case cmd: GetReq =>
            replicator.askGet(
              replyTo => Replicator.Get(CacheKey, readConsistency, replyTo),
              rsp => InternalGet(rsp, cmd)
            )
            Behaviors.same

          case cmd: UpdateReq =>
            cmd match {
              case Put(value)      => updateShape(_.add(node, value))
              case Puts(values)    => updateShape(values.foldLeft(_)((ac, c) => ac.add(node, c)))
              case Remove(value)   => updateShape(_.remove(node, value))
              case Removes(values) => updateShape(values.foldLeft(_)((ac, c) => ac.remove(node, c)))
              case Clear           => updateShape(_.clear(node))
            }
            Behaviors.same

          // get replica successfully
          case InternalGet(rsp @ Replicator.GetSuccess(cacheKey), req) =>
            val set = rsp.get(cacheKey)
            req match {
              case List(reply)            => reply ! set.elements
              case Contains(value, reply) => reply ! set.elements.contains(value)
              case Size(reply)            => reply ! set.size
            }
            Behaviors.same

          // update replica successfully
          case InternalUpdate(_ @Replicator.UpdateSuccess(_)) =>
            Behaviors.same

          // fail to get replica
          case InternalGet(rsp, cmd) =>
            rsp match {
              // prevent wasted time on external ask behavior
              case akka.cluster.ddata.Replicator.NotFound(_, _) =>
                cmd match
                  case List(reply)        => reply ! Set.empty
                  case Contains(_, reply) => reply ! false
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

      inline def list(timeout: Option[Duration] = None): AIO[Set[Value]]                = actor.askZIO(List.apply, timeout)
      inline def size(timeout: Option[Duration] = None): AIO[Int]                       = actor.askZIO(Size.apply, timeout)
      inline def contains(value: Value, timeout: Option[Duration] = None): AIO[Boolean] = actor.askZIO(Contains(value, _), timeout)

      inline def put(value: Value): AIO[Unit]           = actor.tellZIO(Put(value))
      inline def puts(values: Set[Value]): AIO[Unit]    = actor.tellZIO(Puts(values))
      inline def remove(value: Value): AIO[Unit]        = actor.tellZIO(Remove(value))
      inline def removes(values: Set[Value]): AIO[Unit] = actor.tellZIO(Removes(values))
      inline def clear: AIO[Unit]                       = actor.tellZIO(Clear)
    }
