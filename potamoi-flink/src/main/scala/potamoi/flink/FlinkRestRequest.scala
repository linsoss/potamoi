package potamoi.flink

import potamoi.curTs
import potamoi.flink.model.*
import potamoi.flink.FlinkRestErr.{JarNotFound, JobNotFound, RequestApiErr, TaskmanagerNotFound, TriggerNotFound}
import potamoi.flink.FlinkRestRequest.{
  ClusterOverviewInfo,
  JobOverviewInfo,
  JobOverviewRsp,
  JobStatusInfo,
  JobStatusRsp,
  RunJobReq,
  StopJobSptReq,
  TmDetailInfo,
  TriggerSptReq
}
import potamoi.fs.paths
import potamoi.sttps.*
import potamoi.syntax.*
import sttp.client3.basicRequest
import zio.json.{jsonField, DeriveJsonCodec, JsonCodec}
import zio.{IO, Task, ZIO}
import sttp.client3.*
import sttp.client3.ziojson.*

import java.io.File

type JobId     = String
type JarId     = String
type TriggerId = String

val flinkRest = FlinkRestRequest

/**
 * Flink rest api request.
 * Reference to https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/
 */
trait FlinkRestRequest(restUrl: String) {

  /**
   * Uploads jar file.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jars
   */
  def uploadJar(filePath: String): IO[FlinkRestErr, JarId]

  /**
   * Runs job from jar file.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jars-jarid-run
   */
  def runJar(jarId: String, jobRunReq: RunJobReq): IO[FlinkRestErr, JobId]

  /**
   * Deletes jar file.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jars-jarid
   */
  def deleteJar(jarId: String): IO[FlinkRestErr, Unit]

  /**
   * Cancels job.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-1
   */
  def cancelJob(jobId: String): IO[FlinkRestErr, Unit]

  /**
   * Stops job with savepoint.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-stop
   */
  def stopJobWithSavepoint(jobId: String, sptReq: StopJobSptReq): IO[FlinkRestErr, TriggerId]

  /**
   * Triggers a savepoint of job.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-savepoints
   */
  def triggerSavepoint(jobId: String, sptReq: TriggerSptReq): IO[FlinkRestErr, TriggerId]

  /**
   * Get status of savepoint operation.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-savepoints-triggerid
   */
  def getSavepointOperationStatus(jobId: String, triggerId: String): IO[FlinkRestErr, FlinkSptTriggerStatus]

  /**
   * Get all job and the current state.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs
   */
  def listJobsStatusInfo: IO[FlinkRestErr, Vector[JobStatusInfo]]

  /**
   * Get all job overview info
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-overview
   */
  def listJobOverviewInfo: IO[FlinkRestErr, Vector[JobOverviewInfo]]

  /**
   * Get job metrics.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-metrics
   */
  def getJobMetrics(jobId: String, metricsKeys: Set[String]): IO[FlinkRestErr, Map[String, String]]

  /**
   * Get all key of job metrics.
   */
  def getJobMetricsKeys(jobId: String): IO[FlinkRestErr, Set[String]]

  /**
   * Get cluster overview
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#overview-1
   */
  def getClusterOverview: IO[FlinkRestErr, ClusterOverviewInfo]

  /**
   * Get job manager configuration.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobmanager-config
   */
  def getJobmanagerConfig: IO[FlinkRestErr, Map[String, String]]

  /**
   * Get job manager metrics.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobmanager-metrics
   */
  def getJmMetrics(metricsKeys: Set[String]): IO[FlinkRestErr, Map[String, String]]

  /**
   * Get all key of job manager metrics.
   */
  def getJmMetricsKeys: IO[FlinkRestErr, Set[String]]

  /**
   * List all task manager ids on cluster
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#taskmanagers
   */
  def listTaskManagerIds: IO[FlinkRestErr, Vector[String]]

  /**
   * Get task manager detail.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#taskmanagers-taskmanagerid
   */
  def getTaskManagerDetail(tmId: String): IO[FlinkRestErr, TmDetailInfo]

  /**
   * Get task manager metrics.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#taskmanagers-taskmanagerid-metrics
   */
  def getTmMetrics(tmId: String, metricsKeys: Set[String]): IO[FlinkRestErr, Map[String, String]]

  /**
   * Get all key of task manager metrics.
   */
  def getTmMetricsKeys(tmId: String): IO[TaskmanagerNotFound | RequestApiErr, Set[String]]

}

/**
 * Implementation using sttp client.
 */
class FlinkRestRequestLive(restUrl: String) extends FlinkRestRequest(restUrl) {
  import FlinkRestRequest.*
  import FlinkRestErr.*

  private val request = basicRequest

  def uploadJar(filePath: String): IO[RequestApiErr, JarId] =
    usingSttp { backend =>
      request
        .post(uri"$restUrl/jars/upload")
        .multipartBody(
          multipartFile("jarfile", File(filePath))
            .fileName(paths.getFileName(filePath))
            .contentType("application/java-archive")
        )
        .send(backend)
        .flattenBody
        .attemptBody(ujson.read(_)("filename").str.split("/").last)
    } mapError (RequestApiErr("post", s"$restUrl/jars/upload", _))

  def runJar(jarId: String, jobRunReq: RunJobReq): IO[JarNotFound | RequestApiErr, JobId] =
    usingSttp { backend =>
      request
        .post(uri"$restUrl/jars/$jarId/run")
        .body(jobRunReq)
        .send(backend)
        .flattenBody
        .attemptBody(ujson.read(_)("jobid").str)
    } mapError {
      case NotFound => JarNotFound(jarId)
      case err      => RequestApiErr("post", s"$restUrl/jars/$jarId/run", err)
    }

  def deleteJar(jarId: String): IO[JarNotFound | RequestApiErr, Unit] =
    usingSttp { backend =>
      request
        .delete(uri"$restUrl/jars/$jarId")
        .send(backend)
        .unit
    } mapError {
      case NotFound => JarNotFound(jarId)
      case err      => RequestApiErr("delete", s"$restUrl/jars/$jarId", err)
    }

  def cancelJob(jobId: String): IO[JobNotFound | RequestApiErr, Unit] =
    usingSttp { backend =>
      request
        .patch(uri"$restUrl/jobs/$jobId?mode=cancel")
        .send(backend)
        .unit
    } mapError {
      case NotFound => JobNotFound(jobId)
      case err      => RequestApiErr("patch", s"$restUrl/jars/$jobId", err)
    }

  def stopJobWithSavepoint(jobId: String, sptReq: StopJobSptReq): IO[JobNotFound | RequestApiErr, TriggerId] =
    usingSttp { backend =>
      request
        .post(uri"$restUrl/jobs/$jobId/stop")
        .body(sptReq)
        .send(backend)
        .flattenBody
        .attemptBody(ujson.read(_)("request-id").str)
    } mapError {
      case NotFound => JobNotFound(jobId)
      case err      => RequestApiErr("post", s"$restUrl/jobs/$jobId/stop", err)
    }

  def triggerSavepoint(jobId: String, sptReq: TriggerSptReq): IO[JobNotFound | RequestApiErr, TriggerId] =
    usingSttp { backend =>
      request
        .post(uri"$restUrl/jobs/$jobId/savepoints")
        .body(sptReq)
        .send(backend)
        .flattenBody
        .attemptBody(ujson.read(_)("request-id").str)
    } mapError {
      case NotFound => JobNotFound(jobId)
      case err      => RequestApiErr("post", s"$restUrl/jobs/$jobId/savepoints", err)
    }

  def getSavepointOperationStatus(jobId: String, triggerId: String): IO[TriggerNotFound | RequestApiErr, FlinkSptTriggerStatus] =
    usingSttp { backend =>
      request
        .get(uri"$restUrl/jobs/$jobId/savepoints/$triggerId")
        .send(backend)
        .flattenBody
        .attemptBody { body =>
          val rspJson      = ujson.read(body)
          val status       = rspJson("status")("id").str.contra(FlinkPipeOprState.valueOf)
          val failureCause = rspJson("operation").objOpt.map(_("failure-cause")("stack-trace").str)
          FlinkSptTriggerStatus(status, failureCause)
        }
    } mapError {
      case NotFound => TriggerNotFound(triggerId)
      case err      => RequestApiErr("get", s"$restUrl/jobs/$jobId/savepoints/$triggerId", err)
    }

  def listJobsStatusInfo: IO[RequestApiErr, Vector[JobStatusInfo]] =
    usingSttp { backend =>
      request
        .get(uri"$restUrl/jobs")
        .response(asJson[JobStatusRsp])
        .send(backend)
        .flattenBodyT
        .map(_.jobs)
    } mapError (RequestApiErr("get", s"$restUrl/jobs", _))

  def listJobOverviewInfo: IO[RequestApiErr, Vector[JobOverviewInfo]] =
    usingSttp { backend =>
      request
        .get(uri"$restUrl/jobs/overview")
        .response(asJson[JobOverviewRsp])
        .send(backend)
        .flattenBodyT
        .map(_.jobs)
    } mapError (RequestApiErr("get", s"$restUrl/jobs/overview", _))

  def getJobMetrics(jobId: String, metricsKeys: Set[String]): IO[RequestApiErr, Map[String, String]] =
    usingSttp { backend =>
      request
        .get(uri"$restUrl/jobs/$jobId/metrics?get=${metricsKeys.mkString(",")}")
        .send(backend)
        .flattenBody
        .attemptBody(ujson.read(_).arr.map(item => item("id").str -> item("value").str).toMap)
    } mapError (RequestApiErr("get", s"$restUrl/jobs/$jobId/metrics", _))

  def getJobMetricsKeys(jobId: String): IO[RequestApiErr, Set[String]] =
    usingSttp { backend =>
      request
        .get(uri"$restUrl/jobs/$jobId/metrics")
        .send(backend)
        .flattenBody
        .attemptBody(ujson.read(_).arr.map(item => item("id").str).toSet)
    } mapError (RequestApiErr("get", s"$restUrl/jobs/$jobId/metrics", _))

  def getClusterOverview: IO[RequestApiErr, ClusterOverviewInfo] =
    usingSttp { backend =>
      request
        .get(uri"$restUrl/overview")
        .response(asJson[ClusterOverviewInfo])
        .send(backend)
        .flattenBodyT
    } mapError (RequestApiErr("get", s"$restUrl/overview", _))

  def getJobmanagerConfig: IO[RequestApiErr, Map[String, String]] =
    usingSttp { backend =>
      request
        .get(uri"$restUrl/jobmanager/config")
        .send(backend)
        .flattenBody
        .attemptBody(ujson.read(_).arr.map(item => item("key").str -> item("value").str).toMap)
    } mapError (RequestApiErr("get", s"$restUrl/jobmanager/config", _))

  def getJmMetrics(metricsKeys: Set[String]): IO[RequestApiErr, Map[String, String]] =
    usingSttp { backend =>
      request
        .get(uri"$restUrl/jobmanager/metrics?get=${metricsKeys.mkString(",")}")
        .send(backend)
        .flattenBody
        .attemptBody(ujson.read(_).arr.map(item => item("id").str -> item("value").str).toMap)
    } mapError (RequestApiErr("get", s"$restUrl/jobmanager/metrics", _))

  def getJmMetricsKeys: IO[RequestApiErr, Set[String]] =
    usingSttp { backend =>
      request
        .get(uri"$restUrl/jobmanager/metrics")
        .send(backend)
        .flattenBody
        .attemptBody(ujson.read(_).arr.map(item => item("id").str).toSet)
    } mapError (RequestApiErr("get", s"$restUrl/jobmanager/metrics", _))

  def listTaskManagerIds: IO[RequestApiErr, Vector[String]] =
    usingSttp { backend =>
      request
        .get(uri"$restUrl/taskmanagers")
        .send(backend)
        .flattenBody
        .attemptBody(ujson.read(_)("taskmanagers").arr.map(_("id").str).toVector)
    } mapError (RequestApiErr("get", s"$restUrl/taskmanagers", _))

  def getTaskManagerDetail(tmId: String): IO[TaskmanagerNotFound | RequestApiErr, TmDetailInfo] =
    usingSttp { backend =>
      request
        .get(uri"$restUrl/taskmanagers/$tmId")
        .response(asJson[TmDetailInfo])
        .send(backend)
        .flattenBodyT
    } mapError {
      case NotFound => TaskmanagerNotFound(tmId)
      case err      => RequestApiErr("get", s"$restUrl/taskmanagers/$tmId", err)
    }

  def getTmMetrics(tmId: String, metricsKeys: Set[String]): IO[TaskmanagerNotFound | RequestApiErr, Map[String, String]] =
    usingSttp { backend =>
      request
        .get(uri"$restUrl/taskmanagers/$tmId/metrics?get=${metricsKeys.mkString(",")}")
        .send(backend)
        .flattenBody
        .attemptBody(ujson.read(_).arr.map(item => item("id").str -> item("value").str).toMap)
    } mapError {
      case NotFound => TaskmanagerNotFound(tmId)
      case err      => RequestApiErr("get", s"$restUrl/taskmanagers/$tmId/metrics", err)
    }

  def getTmMetricsKeys(tmId: String): IO[TaskmanagerNotFound | RequestApiErr, Set[String]] =
    usingSttp { backend =>
      request
        .get(uri"$restUrl/taskmanagers/$tmId/metrics")
        .send(backend)
        .flattenBody
        .attemptBody(ujson.read(_).arr.map(item => item("id").str).toSet)
    } mapError {
      case NotFound => TaskmanagerNotFound(tmId)
      case err      => RequestApiErr("get", s"$restUrl/taskmanagers/$tmId/metrics", err)
    }
}

object FlinkRestRequest {

  def apply(restUrl: String): FlinkRestRequest = FlinkRestRequestLive(restUrl)

  import potamoi.flink.model.SavepointFormatTypes.given
  import potamoi.flink.model.JobStates.given
  import potamoi.flink.model.FlinkTmDetail.given

  given JsonCodec[RunJobReq]           = DeriveJsonCodec.gen[RunJobReq]
  given JsonCodec[StopJobSptReq]       = DeriveJsonCodec.gen[StopJobSptReq]
  given JsonCodec[TriggerSptReq]       = DeriveJsonCodec.gen[TriggerSptReq]
  given JsonCodec[JobStatusInfo]       = DeriveJsonCodec.gen[JobStatusInfo]
  given JsonCodec[JobStatusRsp]        = DeriveJsonCodec.gen[JobStatusRsp]
  given JsonCodec[TaskStats]           = DeriveJsonCodec.gen[TaskStats]
  given JsonCodec[JobOverviewInfo]     = DeriveJsonCodec.gen[JobOverviewInfo]
  given JsonCodec[JobOverviewRsp]      = DeriveJsonCodec.gen[JobOverviewRsp]
  given JsonCodec[ClusterOverviewInfo] = DeriveJsonCodec.gen[ClusterOverviewInfo]
  given JsonCodec[TmDetailInfo]        = DeriveJsonCodec.gen[TmDetailInfo]

  /**
   * see: [[FlinkRestRequest.runJar]]
   */
  case class RunJobReq(
      @jsonField("entry-class") entryClass: Option[String],
      programArgs: Option[String],
      parallelism: Option[Int],
      savepointPath: Option[String],
      restoreMode: Option[String],
      allowNonRestoredState: Option[Boolean])

  object RunJobReq:
    def apply(jobDef: FlinkSessJobDef): RunJobReq = RunJobReq(
      entryClass = jobDef.appMain,
      programArgs = if (jobDef.appArgs.isEmpty) None else Some(jobDef.appArgs.mkString(" ")),
      parallelism = jobDef.parallelism,
      savepointPath = jobDef.savepointRestore.map(_.savepointPath),
      restoreMode = jobDef.savepointRestore.map(_.restoreMode.toString),
      allowNonRestoredState = jobDef.savepointRestore.map(_.allowNonRestoredState)
    )

  /**
   * see: [[FlinkRestRequest.stopJobWithSavepoint]]
   */
  case class StopJobSptReq(
      drain: Boolean = false,
      formatType: Option[SavepointFormatType] = None,
      targetDirectory: Option[String],
      triggerId: Option[String] = None)

  object StopJobSptReq:
    def apply(sptConf: FlinkJobSavepointDef): StopJobSptReq =
      StopJobSptReq(sptConf.drain, sptConf.formatType, sptConf.savepointPath, sptConf.triggerId)

  /**
   * see: [[FlinkRestRequest.triggerSavepoint]]
   */
  case class TriggerSptReq(
      @jsonField("cancel-job") cancelJob: Boolean = false,
      formatType: Option[SavepointFormatType] = None,
      @jsonField("target-directory") targetDirectory: Option[String],
      triggerId: Option[String] = None)

  object TriggerSptReq:
    def apply(sptConf: FlinkJobSavepointDef): TriggerSptReq =
      TriggerSptReq(cancelJob = false, sptConf.formatType, sptConf.savepointPath, sptConf.triggerId)

  /**
   * see: [[FlinkRestRequest.listJobsStatusInfo]]
   */
  case class JobStatusRsp(jobs: Vector[JobStatusInfo])

  case class JobStatusInfo(id: String, status: JobState)

  /**
   * see: [[FlinkRestRequest.listJobOverviewInfo]]
   */
  case class JobOverviewRsp(jobs: Vector[JobOverviewInfo])

  case class JobOverviewInfo(
      @jsonField("jid") jid: String,
      name: String,
      state: JobState,
      @jsonField("start-time") startTime: Long,
      @jsonField("end-time") endTime: Long,
      @jsonField("last-modification") lastModifyTime: Long,
      tasks: TaskStats):

    def toFlinkJobOverview(fcid: Fcid): FlinkJobOverview =
      model.FlinkJobOverview(
        clusterId = fcid.clusterId,
        namespace = fcid.namespace,
        jobId = jid,
        jobName = name,
        state = state,
        startTs = startTime,
        endTs = endTime,
        tasks = tasks,
        ts = curTs
      )

  case class ClusterOverviewInfo(
      @jsonField("flink-version") flinkVersion: String,
      @jsonField("taskmanagers") taskManagers: Int,
      @jsonField("slots-total") slotsTotal: Int,
      @jsonField("slots-available") slotsAvailable: Int,
      @jsonField("jobs-running") jobsRunning: Int,
      @jsonField("jobs-finished") jobsFinished: Int,
      @jsonField("jobs-cancelled") jobsCancelled: Int,
      @jsonField("jobs-failed") jobsFailed: Int):

    def toFlinkClusterOverview(fcid: Fcid, execMode: FlinkExecMode, deployByPota: Boolean): FlinkClusterOverview =
      model.FlinkClusterOverview(
        clusterId = fcid.clusterId,
        namespace = fcid.namespace,
        execMode = execMode,
        deployByPotamoi = deployByPota,
        tmTotal = taskManagers,
        slotsTotal = slotsTotal,
        slotsAvailable = slotsAvailable,
        jobs = JobsStats(
          running = jobsRunning,
          finished = jobsFinished,
          canceled = jobsCancelled,
          failed = jobsFailed
        ),
        ts = curTs
      )

  case class TmDetailInfo(
      id: String,
      path: String,
      dataPort: Int,
      slotsNumber: Int,
      freeSlots: Int,
      totalResource: TmResource,
      freeResource: TmResource,
      hardware: TmHardware,
      memoryConfiguration: TmMemoryConfig):

    def toTmDetail(fcid: Fcid): FlinkTmDetail = FlinkTmDetail(
      clusterId = fcid.clusterId,
      namespace = fcid.namespace,
      tmId = id,
      path = path,
      dataPort = dataPort,
      slotsNumber = slotsNumber,
      freeSlots = freeSlots,
      totalResource = totalResource,
      freeResource = freeResource,
      hardware = hardware,
      memoryConfiguration = memoryConfiguration,
      ts = curTs
    )
}
