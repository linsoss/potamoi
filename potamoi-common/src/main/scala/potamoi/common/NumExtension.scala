package potamoi.common

object NumExtension {

  /**
   * Int extension
   */
  extension (value: Int) {
    inline def ensureIntMin(min: Int): Int                          = if (value >= min) value else min
    inline def ensureIntOr(cond: Int => Boolean, orValue: Int): Int = if (cond(value)) value else orValue
  }

  /**
   * Double extension
   */
  extension (value: Double) {
    inline def ensureDoubleMin(min: Double): Double                             = if (value >= min) value else min
    inline def ensureDoubleOr(cond: Double => Boolean, orValue: Double): Double = if (cond(value)) value else orValue
  }

}
