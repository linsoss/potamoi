package potamoi.common

import scala.util.Random

object CollectionExtension {

  /**
   * Filter not blank String element and trim it.
   */
  extension (xs: Iterable[String]) def filterNotBlank(): Iterable[String] = {
    xs
      .map(Option(_).map(_.trim).flatMap(s => if (s.isEmpty) None else Some(s)))
      .filter(_.isDefined)
      .map(_.get)
  }

}



