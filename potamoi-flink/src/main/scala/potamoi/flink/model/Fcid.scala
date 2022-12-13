package potamoi.flink.model

import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Unique flink cluster identifier under the same kubernetes cluster.
 */
case class Fcid(clusterId: String, namespace: String):
  def toAnno       = Array("flink.clusterId" -> clusterId, "flink.namespace" -> namespace)
  def show: String = s"clusterId=$clusterId, namespace=$namespace"

object Fcid:
  given JsonCodec[Fcid]                    = DeriveJsonCodec.gen[Fcid]
  given Ordering[Fcid]                     = Ordering.by(fcid => (fcid.clusterId, fcid.namespace))
  given Conversion[(String, String), Fcid] = tuple => Fcid(tuple._1, tuple._2)

/**
 * Unique flink job identifier under the same kubernetes cluster.
 */
case class Fjid(clusterId: String, namespace: String, jobId: String):
  lazy val fcid: Fcid               = Fcid(clusterId, namespace)
  def belongTo(fcid: Fcid): Boolean = fcid.clusterId == clusterId && fcid.namespace == namespace
  def toAnno = Array("flink.clusterId" -> clusterId, "flink.namespace" -> namespace, "flink.jobId" -> jobId)

object Fjid:
  given JsonCodec[Fjid]                      = DeriveJsonCodec.gen[Fjid]
  given Ordering[Fjid]                       = Ordering.by(fjid => (fjid.clusterId, fjid.namespace, fjid.jobId))
  def apply(fcid: Fcid, jobId: String): Fjid = Fjid(fcid.clusterId, fcid.namespace, jobId)

/**
 * Unique flink taskmanager identifier under the same kubernetes cluster.
 */
case class Ftid(clusterId: String, namespace: String, tmId: String):
  lazy val fcid: Fcid      = Fcid(clusterId, namespace)
  def belongTo(fcid: Fcid) = fcid.clusterId == clusterId && fcid.namespace == namespace

object Ftid:
  given JsonCodec[Ftid]                     = DeriveJsonCodec.gen[Ftid]
  given Ordering[Ftid]                      = Ordering.by(ftid => (ftid.clusterId, ftid.namespace, ftid.tmId))
  def apply(fcid: Fcid, tmId: String): Ftid = Ftid(fcid.clusterId, fcid.namespace, tmId)
