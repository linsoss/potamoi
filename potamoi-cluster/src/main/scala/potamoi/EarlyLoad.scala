package potamoi

import zio.URIO

/**
 * Effects that need to be loaded early.
 */
trait EarlyLoad[R]:
  def active: URIO[R, Unit]
