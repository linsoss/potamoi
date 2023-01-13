package potamoi.common

import scala.language.implicitConversions

object Syntax {

  /**
   * Contra control value to the function.
   */
  extension [T](value: T) inline def contra[A](func: T => A): A = func(value)

  extension [T](value: T) inline def tap(func: T => Unit): T = { func(value); value }

  /**
   * Auto convert value to Some
   */
  implicit def valueToSome[T](value: T): Option[T] = Some(value)

  /**
   * Trim String value safely.
   */
  def safeTrim(value: String): String = Option(value).map(_.trim).getOrElse("")

  /**
   * A more reader-friendly version of toString.
   */
  def toPrettyString(value: Any): String =
    value match
      case v: String => v
      case v         => pprint.apply(value, height = 2000).render

  extension (value: AnyRef) def toPrettyStr: String = Syntax.toPrettyString(value)

}
