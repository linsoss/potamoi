package com.github.potamois.potamoi.gateway.flink

import org.apache.flink.table.api.TableEnvironment

/**
 * Flink version information
 *
 * @param major   major version likes "1.14"
 * @param version full version likes "1.14.0"
 * @author Al-assad
 */
case class FlinkVersion(major: String, version: String) {

  // construct from flink full version string like "1.14.0"
  def this(version: String) = this(version.split('.').take(2).mkString("."), version)

  // Major version sign likes "114" for version "1.14"
  lazy val majorSign: Int = major.split('.').mkString("").toInt
}


object FlinkVersion {

  /**
   * Current flink version on the system, which is determined by the version
   * information of the jar where the [[TableEnvironment]] is actually loaded.
   */
  lazy val curSystemFlinkVers: FlinkVersion =
    new FlinkVersion(classOf[TableEnvironment].getPackage.getImplementationVersion)

}


