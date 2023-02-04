package potamoi.flink

import potamoi.{codecs, curTs}
import potamoi.flink.model.*
import potamoi.flink.model.snapshot.JobStates.given_JsonCodec_JobState
import potamoi.flink.FlinkRestErr.*
import potamoi.flink.FlinkRestRequest.*
import potamoi.flink.model.deploy.{FlinkJobSavepointDef, FlinkSessJobDef, SavepointFormatType}
import potamoi.flink.model.snapshot.{FlinkClusterOverview, FlinkJobOverview, FlinkPipeOprStates, FlinkSptTriggerStatus, FlinkTmDetail, JobsStats, JobState, TaskStats, TmHardware, TmMemoryConfig, TmResource}
import potamoi.fs.paths
import potamoi.sttps.*
import potamoi.syntax.*
import sttp.client3.*
import sttp.client3.ziojson.*
import zio.{IO, Task, UIO, ZIO}
import zio.json.{jsonField, DeriveJsonCodec, JsonCodec}

import java.io.File

type JobId     = String
type JarId     = String
type TriggerId = String

val flinkRest = FlinkRestRequest

/**
 * Flink rest api request.
 * Reference to https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/
 */
class FlinkRestRequest(restUrl: String) {

  private val request = basicRequest

  /**
   * Check the availability of rest api.
   */
  def isAvailable: UIO[Boolean] = usingSttp { backend =>
    request
      .get(uri"$restUrl/config")
      .send(backend)
      .flattenBody
      .as(true)
  } catchAll (_ => ZIO.succeed(false))

  /**
   * Uploads jar file.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jars
   */
  def uploadJar(filePath: String): IO[RequestApiErr, JarId] = usingSttp { backend =>
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

  /**
   * Runs job from jar file.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jars-jarid-run
   */
  def runJar(jarId: String, jobRunReq: RunJobReq): IO[(RequestApiErr | JarNotFound) with FlinkRestErr, JobId] =
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

  /**
   * Deletes jar file.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jars-jarid
   */
  def deleteJar(jarId: String): IO[(JarNotFound | RequestApiErr) with FlinkRestErr, Unit] =
    usingSttp { backend =>
      request
        .delete(uri"$restUrl/jars/$jarId")
        .send(backend)
        .unit
    } mapError {
      case NotFound => JarNotFound(jarId)
      case err      => RequestApiErr("delete", s"$restUrl/jars/$jarId", err)
    }

  /**
   * Cancels job.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-1
   */
  def cancelJob(jobId: String): IO[(JobNotFound | RequestApiErr) with FlinkRestErr, Unit] =
    usingSttp { backend =>
      request
        .patch(uri"$restUrl/jobs/$jobId?mode=cancel")
        .send(backend)
        .unit
    } mapError {
      case NotFound => JobNotFound(jobId)
      case err      => RequestApiErr("patch", s"$restUrl/jars/$jobId", err)
    }

  /**
   * Stops job with savepoint.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-stop
   */
  def stopJobWithSavepoint(jobId: String, sptReq: StopJobSptReq): IO[(JobNotFound | RequestApiErr) with FlinkRestErr, TriggerId] =
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

  /**
   * Triggers a savepoint of job.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-savepoints
   */
  def triggerSavepoint(jobId: String, sptReq: TriggerSptReq): IO[(JobNotFound | RequestApiErr) with FlinkRestErr, TriggerId] =
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

  /**
   * Get status of savepoint operation.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-savepoints-triggerid
   */
  def getSavepointOperationStatus(jobId: String, triggerId: String): IO[(TriggerNotFound | RequestApiErr) with FlinkRestErr, FlinkSptTriggerStatus] =
    usingSttp { backend =>
      request
        .get(uri"$restUrl/jobs/$jobId/savepoints/$triggerId")
        .send(backend)
        .flattenBody
        .attemptBody { body =>
          val rspJson                  = ujson.read(body)
          val status                   = rspJson("status")("id").str.contra(FlinkPipeOprStates.ofRaw)
          val (location, failureCause) = rspJson("operation").objOpt match
            case None            => None -> None
            case Some(operation) =>
              println(operation)
              val loc     = operation.get("location").flatMap(_.strOpt)
              val failure = operation.get("failure-cause").flatMap(_.objOpt.flatMap(_.get("stack-trace").strOpt))
              loc -> failure
          FlinkSptTriggerStatus(status, failureCause, location)
        }
    } mapError {
      case NotFound => TriggerNotFound(triggerId)
      case err      => RequestApiErr("get", s"$restUrl/jobs/$jobId/savepoints/$triggerId", err)
    }

  /**
   * Get all job and the current state.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs
   */
  def listJobsStatusInfo: IO[RequestApiErr, Vector[JobStatusInfo]] = usingSttp { backend =>
    request
      .get(uri"$restUrl/jobs")
      .response(asJson[JobStatusRsp])
      .send(backend)
      .flattenBodyT
      .map(_.jobs)
  } mapError (RequestApiErr("get", s"$restUrl/jobs", _))

  /**
   * Get all job overview info
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-overview
   */
  def listJobOverviewInfo: IO[RequestApiErr, Vector[JobOverviewInfo]] = usingSttp { backend =>
    request
      .get(uri"$restUrl/jobs/overview")
      .response(asJson[JobOverviewRsp])
      .send(backend)
      .flattenBodyT
      .map(_.jobs)
  } mapError (RequestApiErr("get", s"$restUrl/jobs/overview", _))

  /**
   * Get job metrics.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobs-jobid-metrics
   */
  def getJobMetrics(jobId: String, metricsKeys: Set[String]): IO[RequestApiErr, Map[String, String]] =
    usingSttp { backend =>
      request
        .get(uri"$restUrl/jobs/$jobId/metrics?get=${metricsKeys.mkString(",")}")
        .send(backend)
        .flattenBody
        .attemptBody(ujson.read(_).arr.map(item => item("id").str -> item("value").str).toMap)
    } mapError (RequestApiErr("get", s"$restUrl/jobs/$jobId/metrics", _))

  /**
   * Get all key of job metrics.
   */
  def getJobMetricsKeys(jobId: String): IO[RequestApiErr, Set[String]] = usingSttp { backend =>
    request
      .get(uri"$restUrl/jobs/$jobId/metrics")
      .send(backend)
      .flattenBody
      .attemptBody(ujson.read(_).arr.map(item => item("id").str).toSet)
  } mapError (RequestApiErr("get", s"$restUrl/jobs/$jobId/metrics", _))

  /**
   * Get cluster overview
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#overview-1
   */
  def getClusterOverview: IO[RequestApiErr, ClusterOverviewInfo] = usingSttp { backend =>
    request
      .get(uri"$restUrl/overview")
      .response(asJson[ClusterOverviewInfo])
      .send(backend)
      .flattenBodyT
  } mapError (RequestApiErr("get", s"$restUrl/overview", _))

  /**
   * Get job manager configuration.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobmanager-config
   */
  def getJobmanagerConfig: IO[RequestApiErr, Map[String, String]] = usingSttp { backend =>
    request
      .get(uri"$restUrl/jobmanager/config")
      .send(backend)
      .flattenBody
      .attemptBody(ujson.read(_).arr.map(item => item("key").str -> item("value").str).toMap)
  } mapError (RequestApiErr("get", s"$restUrl/jobmanager/config", _))

  /**
   * Get job manager metrics.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#jobmanager-metrics
   */
  def getJmMetrics(metricsKeys: Set[String]): IO[RequestApiErr, Map[String, String]] =
    usingSttp { backend =>
      request
        .get(uri"$restUrl/jobmanager/metrics?get=${metricsKeys.mkString(",")}")
        .send(backend)
        .flattenBody
        .attemptBody(ujson.read(_).arr.map(item => item("id").str -> item("value").str).toMap)
    } mapError (RequestApiErr("get", s"$restUrl/jobmanager/metrics", _))

  /**
   * Get all key of job manager metrics.
   */
  def getJmMetricsKeys: IO[RequestApiErr, Set[String]] = usingSttp { backend =>
    request
      .get(uri"$restUrl/jobmanager/metrics")
      .send(backend)
      .flattenBody
      .attemptBody(ujson.read(_).arr.map(item => item("id").str).toSet)
  } mapError (RequestApiErr("get", s"$restUrl/jobmanager/metrics", _))

  /**
   * List all task manager ids on cluster
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#taskmanagers
   */
  def listTaskManagerIds: IO[RequestApiErr, Vector[String]] = usingSttp { backend =>
    request
      .get(uri"$restUrl/taskmanagers")
      .send(backend)
      .flattenBody
      .attemptBody(ujson.read(_)("taskmanagers").arr.map(_("id").str).toVector)
  }.catchSome { case NotFound => ZIO.succeed(Vector.empty) }
    .mapError(RequestApiErr("get", s"$restUrl/taskmanagers", _))

  /**
   * Get task manager detail.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#taskmanagers-taskmanagerid
   */
  def getTaskManagerDetail(tmId: String): IO[(TaskmanagerNotFound | RequestApiErr) with FlinkRestErr, TmDetailInfo] =
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

  /**
   * Get task manager metrics.
   * see: https://nightlies.apache.org/flink/flink-docs-master/docs/ops/rest_api/#taskmanagers-taskmanagerid-metrics
   */
  def getTmMetrics(tmId: String, metricsKeys: Set[String]): IO[(TaskmanagerNotFound | RequestApiErr) with FlinkRestErr, Map[String, String]] =
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

  /**
   * Get all key of task manager metrics.
   */
  def getTmMetricsKeys(tmId: String): IO[(TaskmanagerNotFound | RequestApiErr) with FlinkRestErr, Set[String]] =
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

  def apply(restUrl: String): FlinkRestRequest = new FlinkRestRequest(restUrl)

  // ------------------------ flank rest api request/response object ----------------------------------------------

  given JsonCodec[SavepointFormatType] = codecs.stringBasedJsonCodec(_.rawValue, s => SavepointFormatType.values.find(_.rawValue == s))

  /**
   * see: [[FlinkRestRequest.runJar]]
   */
  case class RunJobReq(
      jobId: Option[String],
      @jsonField("entry-class") entryClass: Option[String],
      programArgs: Option[String],
      parallelism: Option[Int],
      savepointPath: Option[String],
      restoreMode: Option[String],
      allowNonRestoredState: Option[Boolean])
      derives JsonCodec

  object RunJobReq:
    def apply(jobDef: FlinkSessJobDef): RunJobReq = RunJobReq(
      jobId = jobDef.jobId,
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
      derives JsonCodec

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
      derives JsonCodec

  object TriggerSptReq:
    def apply(sptConf: FlinkJobSavepointDef): TriggerSptReq =
      TriggerSptReq(cancelJob = false, sptConf.formatType, sptConf.savepointPath, sptConf.triggerId)

  /**
   * see: [[FlinkRestRequest.listJobsStatusInfo]]
   */
  case class JobStatusRsp(jobs: Vector[JobStatusInfo]) derives JsonCodec

  case class JobStatusInfo(id: String, status: JobState) derives JsonCodec

  /**
   * see: [[FlinkRestRequest.listJobOverviewInfo]]
   */
  case class JobOverviewRsp(jobs: Vector[JobOverviewInfo]) derives JsonCodec

  case class JobOverviewInfo(
      @jsonField("jid") jid: String,
      name: String,
      state: JobState,
      @jsonField("start-time") startTime: Long,
      @jsonField("end-time") endTime: Long,
      @jsonField("last-modification") lastModifyTime: Long,
      tasks: TaskStats)
      derives JsonCodec:

    def toFlinkJobOverview(fcid: Fcid): FlinkJobOverview =
      FlinkJobOverview(
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
      @jsonField("jobs-failed") jobsFailed: Int)
      derives JsonCodec:

    def toFlinkClusterOverview(fcid: Fcid, execType: FlinkTargetType, deployByPota: Boolean): FlinkClusterOverview =
      FlinkClusterOverview(
        clusterId = fcid.clusterId,
        namespace = fcid.namespace,
        execType = execType,
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
      memoryConfiguration: TmMemoryConfig)
      derives JsonCodec:

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
