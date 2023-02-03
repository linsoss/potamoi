package potamoi.akka

import akka.actor.typed.ActorRef
import potamoi.KryoSerializable

import scala.util.Either

object AkkaClusterTool {}

type ValueReply[A]     = ActorRef[ValuePack[A]]
type EitherReply[E, A] = ActorRef[EitherPack[E, A]]

case object Ack                                  extends KryoSerializable
case class ValuePack[A](value: A)                extends KryoSerializable
case class EitherPack[E, A](value: Either[E, A]) extends KryoSerializable

def pack[A](value: A): ValuePack[A]                         = ValuePack(value)
def packEither[E, A](value: Either[E, A]): EitherPack[E, A] = EitherPack(value)
