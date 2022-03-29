package com.github.potamois.potamoi.gateway.flink

import com.github.potamois.potamoi.gateway.flink.FlinkVersion.FlinkVerSign
import org.apache.flink.table.api.TableEnvironment

/**
 * Flink version information
 *
 * @param major   major version likes "1.14"
 * @param version full version likes "1.14.0"
 * @author Al-assad
 */
case class FlinkVersion(major: String, version: String) {

  /**
   * Construct from flink full version string like "1.14.0"
   */
  def this(version: String) = this(version.split('.').take(2).mkString("."), version)

  /**
   * Major version sign likes "114" for version "1.14"
   */
  lazy val majorSign: FlinkVerSign = major.split('.').mkString("").toInt
}


object FlinkVersion {

  /**
   * See [[FlinkVersion.majorSign]]
   */
  type FlinkVerSign = Int

  /**
   * Current flink version on the system, which is determined by the version
   * information of the jar where the [[TableEnvironment]] is actually loaded.
   */
  lazy val SystemFlinkVer: FlinkVersion =
    new FlinkVersion(classOf[TableEnvironment].getPackage.getImplementationVersion)

  /**
   * [[FlinkVerSign]] of the current system, likes "114" for version "1.14"
   */
  lazy val SystemFlinkVerSign: FlinkVerSign = SystemFlinkVer.majorSign

  /**
   * Major versions sign of flink supported by potamoi, see [[FlinkVersion.majorSign]].
   */
  val FlinkVerSignRange: Seq[FlinkVerSign] = Seq(114, 113, 112, 111)

}


