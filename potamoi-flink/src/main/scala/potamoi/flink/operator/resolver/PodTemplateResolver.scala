package potamoi.flink.operator.resolver

import com.coralogix.zio.k8s.model.core.v1.*
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.ObjectMeta
import io.circe.syntax.*
import io.circe.yaml.parser.parse as parseYaml
import io.circe.yaml.syntax.*
import potamoi.flink.FlinkConf
import potamoi.flink.ResolveFlinkClusterDefErr.ResolvePodTemplateErr
import potamoi.flink.model.{FlinkAppClusterDef, FlinkClusterDef}
import potamoi.fs.{lfs, S3Conf}
import potamoi.fs.PathTool.{isS3Path, purePath}
import zio.{IO, ZIO}
import zio.ZIO.logInfo
import zio.prelude.data.Optional.{Absent, Present}

/**
 * Flink K8s PodTemplate resolver.
 */
object PodTemplateResolver {

  /**
   * Generate PodTemplate and dump it to local dir, return the yaml file path on local fs.
   */
  def resolvePodTemplateAndDump(
      definition: FlinkClusterDef[_],
      flinkConf: FlinkConf,
      s3Conf: S3Conf,
      outputPath: String): IO[ResolvePodTemplateErr, Unit] =
    for {
      podTemplate <- resolvePodTemplate(definition, flinkConf: FlinkConf, s3Conf: S3Conf)
      _           <- writeToLocal(podTemplate, outputPath)
    } yield ()

  /**
   * Resolve and generate PodTemplate from Flink cluster definition,
   * if definition.overridePodTemplate is defined, use it directly.
   */
  def resolvePodTemplate(definition: FlinkClusterDef[_], flinkConf: FlinkConf, s3Conf: S3Conf): IO[ResolvePodTemplateErr, Pod] =
    definition.overridePodTemplate match {
      case None => genPodTemplate(definition, flinkConf, s3Conf)
      case Some(podTemplate) =>
        ZIO
          .fromEither(parseYaml(podTemplate).map(_.as[Pod]).flatMap(identity))
          .mapError(ResolvePodTemplateErr("Unable to decode podTemplate content", _))
    }

  /**
   * Generate PodTemplate from Flink cluster definition and ignore definition.overridePodTemplate.
   */
  def genPodTemplate(definition: FlinkClusterDef[_], flinkConf: FlinkConf, s3Conf: S3Conf): IO[ResolvePodTemplateErr, Pod] =
    ZIO
      .attempt {
        // user libs on s3: (pure path, jar name)
        val splitPath: String => (String, String) = path => purePath(path) -> path.split('/').last
        val libsOnS3 = {
          val thirdPartyLibs = definition.injectedDeps.filter(isS3Path).map(splitPath)
          val jobJarLib = definition match
            case app: FlinkAppClusterDef => Option(app.jobJar).filter(isS3Path).map(splitPath)
            case _                       => None
          thirdPartyLibs ++ jobJarLib
        }

        // userlib-loader initContainer
        lazy val cpS3LibClauses = libsOnS3
          .map { case (path, name) => s3Conf.revisePath(path) -> name }
          .map { case (path, name) => s"&& mc cp minio/$path /opt/flink/lib/$name" }
          .mkString(" ")

        val libLoaderInitContainer =
          if (libsOnS3.isEmpty) None
          else
            Some(
              Container(
                name = "userlib-loader",
                image = flinkConf.minioClientImage,
                command = Vector("sh", "-c", s"mc alias set minio ${s3Conf.endpoint} ${s3Conf.accessKey} ${s3Conf.secretKey} $cpS3LibClauses"),
                volumeMounts = Vector(VolumeMount(name = "flink-libs", mountPath = "/opt/flink/lib")))
            )
        val initContainers = Vector(libLoaderInitContainer).flatten

        // pod template definition
        val podDef = Pod(
          metadata = ObjectMeta(name = "pod-template"),
          spec = PodSpec(
            volumes = Vector(
              Volume(name = "flink-volume-hostpath", hostPath = HostPathVolumeSource(path = "/tmp", `type` = "Directory")),
              Volume(name = "flink-libs", emptyDir = EmptyDirVolumeSource()),
              Volume(name = "flink-logs", emptyDir = EmptyDirVolumeSource())
            ),
            initContainers = if (initContainers.isEmpty) Absent else Present(initContainers),
            containers = Vector(
              Container(
                name = "flink-main-container",
                volumeMounts = Vector(
                  VolumeMount(name = "flink-volume-hostpath", mountPath = "/opt/flink/volume"),
                  VolumeMount(name = "flink-logs", mountPath = "/opt/flink/log"),
                ) ++ libsOnS3.map { case (_, name) =>
                  VolumeMount(name = "flink-libs", mountPath = s"/opt/flink/lib/$name", subPath = name)
                })
            ))
        )
        podDef
      }
      .mapError(ResolvePodTemplateErr("Unable to generate flink pod template spec", _))

  /**
   * Encode podTemplate object to yaml string content.
   */
  def encodePodTemplateToYaml(podTemplate: Pod): IO[ResolvePodTemplateErr, String] =
    ZIO
      .attempt(podTemplate.asJson.deepDropNullValues.asYaml.spaces2)
      .mapError(ResolvePodTemplateErr("Unable to encode flink pod template to yaml", _))

  /**
   * Write the Pod to a local temporary file in yaml format.
   * Return generated yaml file path.
   */
  def writeToLocal(podTemplate: Pod, path: String): IO[ResolvePodTemplateErr, Unit] =
    for {
      yaml <- encodePodTemplateToYaml(podTemplate)
      _    <- (lfs.rm(path) *> lfs.write(path, yaml)).mapError(e => ResolvePodTemplateErr(s"Fail to write podtemplate to local file: $path", e.cause))
      _    <- logInfo(s"Wrote flink cluster podtemplate to local file: $path")
    } yield ()

}
