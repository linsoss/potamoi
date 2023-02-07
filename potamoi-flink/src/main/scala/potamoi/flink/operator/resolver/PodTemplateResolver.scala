package potamoi.flink.operator.resolver

import com.coralogix.zio.k8s.model.core.v1.*
import com.coralogix.zio.k8s.model.pkg.apis.meta.v1.ObjectMeta
import io.circe.syntax.*
import io.circe.yaml.parser.parse as parseYaml
import io.circe.yaml.syntax.*
import potamoi.{BaseConf, PotaErr}
import potamoi.flink.FlinkConf
import potamoi.flink.ResolveFlinkClusterSpecErr.ResolvePodTemplateErr
import potamoi.flink.model.deploy.{ClusterSpec, JarAppClusterSpec}
import potamoi.fs.{lfs, paths, FileServer, FileServerConf}
import potamoi.fs.FsErr.UnSupportedSchema
import potamoi.zios.asLayer
import zio.{IO, URIO, ZIO}
import zio.prelude.data.Optional.{Absent, Present}
import zio.ZIO.{logInfo, succeed}

/**
 * Flink Kubernetes pod-template resolver.
 */
object PodTemplateResolver {

  /**
   * Generate PodTemplate and dump it to local dir, return the yaml file path on local fs.
   */
  def resolvePodTemplateAndDump(
      spec: ClusterSpec,
      outputPath: String): ZIO[FlinkConf with FileServerConf with BaseConf, ResolvePodTemplateErr, Unit] = {
    for
      podTemplate <- resolvePodTemplate(spec)
      _           <- writeToLocal(podTemplate, outputPath)
    yield ()
  }

  /**
   * Resolve and generate PodTemplate from Flink cluster definition,
   * if the [[ClusterSpec.props.overridePodTemplate]] is defined, use it directly.
   */
  def resolvePodTemplate(spec: ClusterSpec): ZIO[FlinkConf with FileServerConf with BaseConf, ResolvePodTemplateErr, Pod] = {
    spec.props.overridePodTemplate match
      case None              => genPodTemplate(spec)
      case Some(podTemplate) =>
        ZIO
          .fromEither(parseYaml(podTemplate).map(_.as[Pod]).flatMap(identity))
          .mapError(ResolvePodTemplateErr("Unable to decode podTemplate content", _))
  }

  /**
   * Generate PodTemplate from Flink cluster spec.
   */
  def genPodTemplate(spec: ClusterSpec): URIO[FlinkConf with FileServerConf with BaseConf, Pod] = {
    for {
      flinkConf <- ZIO.service[FlinkConf]

      // collect user lib on pota fs from spec
      thirtyPartyLibsPota = spec.props.udfJars.filter(paths.isPotaPath)
      jobJarLibPota       = spec match
                              case jarAppSpec: JarAppClusterSpec => Option(jarAppSpec.jobJar).filter(paths.isPotaPath)
                              case _                             => None
      userLibsOnPota      = thirtyPartyLibsPota ++ jobJarLibPota
      // file-name -> pota file-path
      userLibsPaths       = userLibsOnPota.map(path => paths.getFileName(path) -> path)

      // file-name -> pota http file-path
      userLibsHttps: Set[(String, String)] <- ZIO
                                                .foreach(userLibsPaths) { case (fileName, potaPath) =>
                                                  FileServer
                                                    .getRemoteHttpFilePath(potaPath)
                                                    .catchAll(_ => succeed(""))
                                                    .map(httpPath => fileName -> httpPath)
                                                }
                                                .map(_.filter(_._2.nonEmpty))
      _                                    <- ZIO.unit

      // userlib-loader initContainer
      libDownloadCmdArgs     = userLibsHttps.flatMap { case (fileName, httpPath) => Vector(httpPath, "-O", s"/opt/flink/lib/$fileName") }
      libLoaderInitContainer = if userLibsHttps.isEmpty then None
                               else
                                 Some(
                                   Container(
                                     name = "userlib-loader",
                                     image = flinkConf.busyBoxImage,
                                     command = Vector("wget") ++ libDownloadCmdArgs.toVector,
                                     volumeMounts = Vector(VolumeMount(name = "flink-libs", mountPath = "/opt/flink/lib"))
                                   ))
      initContainers         = Vector(libLoaderInitContainer).flatten

      // pod template definition
      podSpec = Pod(
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
                        ) ++ userLibsHttps.map { case (fileName, _) =>
                          VolumeMount(name = "flink-libs", mountPath = s"/opt/flink/lib/$fileName", subPath = fileName)
                        })
                    ))
                )
    } yield podSpec
  }

  /**
   * Encode podTemplate object to yaml string content.
   */
  def encodePodTemplateToYaml(podTemplate: Pod): IO[ResolvePodTemplateErr, String] = {
    ZIO
      .attempt(podTemplate.asJson.deepDropNullValues.asYaml.spaces2)
      .mapError(ResolvePodTemplateErr("Unable to encode flink pod template to yaml", _))
  }

  /**
   * Write the Pod to a local temporary file in yaml format.
   * Return generated yaml file path.
   */
  def writeToLocal(podTemplate: Pod, path: String): IO[ResolvePodTemplateErr, Unit] = {
    for
      yaml <- encodePodTemplateToYaml(podTemplate)
      _    <- (lfs.rm(path) *> lfs.write(path, yaml)).mapError(e => ResolvePodTemplateErr(s"Fail to write podtemplate to local file: $path", e.cause))
      _    <- logInfo(s"Wrote flink pod template to local file: $path")
    yield ()
  }

}
