package potamoi.akka

import akka.cluster.ddata.Replicator
import potamoi.{codecs, common}
import potamoi.akka.DDataReadLevels.given_JsonCodec_DDataReadLevel
import potamoi.akka.DDataWriteLevels.given_JsonCodec_DDataWriteLevel
import potamoi.codecs.scalaDurationJsonCodec
import potamoi.times.given_Conversion_ScalaDuration_FiniteDuration
import zio.config.magnolia.name
import zio.json.{DeriveJsonCodec, JsonCodec}

import scala.concurrent.duration.{Duration, DurationInt}

/**
 * Akka distributed data configuration.
 */
case class DDataConf(
    @name("read-level") readLevel: DDataReadLevel = DDataReadLevel.Local,
    @name("write-level") writeLevel: DDataWriteLevel = DDataWriteLevel.Majority,
    @name("replica-timeout") replicaTimeout: Duration = 30.seconds,
    @name("log-setup-info") logSetupInfo: Boolean = true)
    derives JsonCodec:

  lazy val writeConsistency: Replicator.WriteConsistency = readLevel match {
    case DDataReadLevel.Local    => Replicator.WriteLocal
    case DDataReadLevel.Majority => Replicator.WriteMajority(timeout = replicaTimeout, minCap = 0)
    case DDataReadLevel.All      => Replicator.WriteAll(timeout = replicaTimeout)
  }

  lazy val readConsistency: Replicator.ReadConsistency = readLevel match {
    case DDataReadLevel.Local    => Replicator.ReadLocal
    case DDataReadLevel.Majority => Replicator.ReadMajority(timeout = replicaTimeout, minCap = 0)
    case DDataReadLevel.All      => Replicator.ReadAll(timeout = replicaTimeout)
  }

object DDataConf:
  lazy val default = DDataConf(logSetupInfo = false)

/**
 * Akka distributed data write level.
 */
enum DDataWriteLevel:
  case Local
  case Majority
  case All

/**
 * Akka distributed data read level.
 */
enum DDataReadLevel:
  case Local
  case Majority
  case All

object DDataWriteLevels:
  given JsonCodec[DDataWriteLevel] = codecs.simpleEnumJsonCodec(DDataWriteLevel.values)

object DDataReadLevels:
  given JsonCodec[DDataReadLevel] = codecs.simpleEnumJsonCodec(DDataReadLevel.values)
