package potamoi.common

import scala.reflect.{classTag, ClassTag}

object OptionExtension:

  /**
   * Get value from option or throw exception.
   */
  extension [A: ClassTag](opt: Option[A])
    @throws[Exception]
    def unsafeGet = opt.getOrElse(throw new Exception(s"${classTag[A].runtimeClass} not yet initialized."))
