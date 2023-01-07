package potamoi.logger

import org.slf4j.LoggerFactory
import zio.{ZIO, ZIOAppDefault, ZIOAspect}
import zio.Console.printLine
import zio.ZIO.logInfo
import zio.stream.ZStream

object PotaLoggerTest1 extends ZIOAppDefault:
  private val logger = LoggerFactory.getLogger(this.getClass)
  val run = {
    ZIO.logDebug("debug-msg") *>
    ZIO.logInfo("info-msg") *>
    ZIO.logWarning("warning-msg") *>
    ZIO.logError("fail-msg") *>
    ZIO.succeed(logger.debug("slf4j-debug-msg")) *>
    ZIO.succeed(logger.info("slf4j-info-msg")) *>
    ZIO.succeed(logger.warn("slf4j-warn-msg"))
  }.provide(PotaLogger.layer(level = LogsLevel.Debug))

object PotaLoggerTest2 extends ZIOAppDefault:
  val run = {
    ZIO.logInfo("msg1") *>
    ZIO.logInfo("msg2") @@ ZIOAspect.annotated("k1", "v1") *>
    ZIO.logInfo("msg3")
  }.provide(PotaLogger.layer())
