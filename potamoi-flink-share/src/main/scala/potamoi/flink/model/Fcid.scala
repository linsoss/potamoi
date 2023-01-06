package potamoi.flink.model

import zio.json.JsonCodec

/**
 * Unique flink cluster identifier under the same kubernetes cluster.
 */
case class Fcid(clusterId: String, namespace: String) derives JsonCodec:
  def toAnno       = Array("flink.clusterId" -> clusterId, "flink.namespace" -> namespace)
  def show: String = s"clusterId=$clusterId, namespace=$namespace"

object Fcid:
  given Conversion[(String, String), Fcid] = tuple => Fcid(tuple._1, tuple._2)

/**
 * Unique flink job identifier under the same kubernetes cluster.
 */
case class Fjid(clusterId: String, namespace: String, jobId: String) derives JsonCodec:
  lazy val fcid: Fcid               = Fcid(clusterId, namespace)
  def belongTo(fcid: Fcid): Boolean = fcid.clusterId == clusterId && fcid.namespace == namespace
  def toAnno                        = Array("flink.clusterId" -> clusterId, "flink.namespace" -> namespace, "flink.jobId" -> jobId)

object Fjid:
  def apply(fcid: Fcid, jobId: String): Fjid = Fjid(fcid.clusterId, fcid.namespace, jobId)

/**
 * Unique flink taskmanager identifier under the same kubernetes cluster.
 */
case class Ftid(clusterId: String, namespace: String, tmId: String) derives JsonCodec:
  lazy val fcid: Fcid      = Fcid(clusterId, namespace)
  def belongTo(fcid: Fcid) = fcid.clusterId == clusterId && fcid.namespace == namespace

object Ftid:
  def apply(fcid: Fcid, tmId: String): Ftid = Ftid(fcid.clusterId, fcid.namespace, tmId)
