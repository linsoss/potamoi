package potamoi.flink.storage

import zio.{Ref, UIO}
import zio.stream.{Stream, ZStream}

import scala.collection.mutable
import scala.reflect.ClassTag

package object mem:

  /**
   * Thread-safe map based in-memory storage.
   */
  private[mem] case class MapBasedStg[Key: ClassTag, Value: ClassTag](ref: Ref[mutable.Map[Key, Value]]) {
    def get(key: Key): UIO[Option[Value]]                                  = ref.get.map(_.get(key))
    def getByKey(f: Key => Boolean): UIO[List[Value]]                      = ref.get.map(_.filter { case (k, _) => f(k) }.values.toList)
    def getPartByKey[A](f: Key => Boolean, part: Value => A): UIO[List[A]] = ref.get.map(_.filter { case (k, _) => f(k) }.values.map(part).toList)
    def getPart[A](key: Key, part: Value => A): UIO[Option[A]]             = ref.get.map(_.get(key).map(part))

    def getKeys: UIO[List[Key]]              = ref.get.map(_.keys.toList)
    def getValues: UIO[List[Value]]          = ref.get.map(_.values.toList)
    def streamValues: Stream[Nothing, Value] = ZStream.fromIterableZIO(ref.get.map(_.values))

    def put(key: Key, value: Value): UIO[Unit] = ref.get.map(_ += (key -> value))
    def putAll(kv: Map[Key, Value]): UIO[Unit] = ref.get.map(_ ++= kv)
    def delete(key: Key): UIO[Unit]            = ref.get.map(_ -= key)

    def deleteByKey(f: Key => Boolean): UIO[Unit] = ref.update { map =>
      map.keys
        .filter(f(_))
        .foldLeft(map)((ac, a) => ac -= a)
    }
  }
