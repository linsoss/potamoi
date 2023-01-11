package potamoi.common

import scala.util.Random
import zio.Ref

import scala.collection.mutable

object CollectionExtension:

  /**
   * Filter not blank String element and trim it.
   */
  extension (xs: Iterable[String])
    def filterNotBlank(): Iterable[String] =
      xs
        .map(Option(_).map(_.trim).flatMap(s => if (s.isEmpty) None else Some(s)))
        .filter(_.isDefined)
        .map(_.get)

  /**
   * Update value of Ref[Map].
   */
  extension [K, V](mapRef: Ref[mutable.Map[K, V]])
    inline def updateWith(key: K, modify: V => V) = mapRef.update { map =>
      map.get(key) match
        case Some(value) => map += (key -> modify(value))
        case None        => map
    }
