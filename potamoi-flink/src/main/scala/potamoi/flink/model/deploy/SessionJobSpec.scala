package potamoi.flink.model.deploy

import potamoi.flink.model.deploy.SavepointRestoreProp
import potamoi.flink.model.Fcid

/**
 * Definition of the job submitted to Flink session cluster.
 */
case class SessionJobSpec(
    clusterId: String,
    namespace: String,
    jobJar: String,
    jobId: Option[String] = None,
    appMain: Option[String] = None,
    appArgs: List[String] = List.empty,
    parallelism: Option[Int] = None,
    savepointRestore: Option[SavepointRestoreProp] = None):
  val fcid: Fcid = clusterId -> namespace
