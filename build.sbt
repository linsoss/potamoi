lazy val scala3   = "3.2.1"
lazy val scala213 = "2.13.10"
lazy val scala212 = "2.12.17"
lazy val javaVer  = "17"

lazy val zioVer        = "2.0.4"
lazy val zioLoggingVer = "2.1.5"
lazy val zioConfig     = "3.0.2"
lazy val zioJsonVer    = "0.3.0"
lazy val zioHttpVer    = "0.0.3"
lazy val zioK8sVer     = "2.0.1"
lazy val zioDirect     = "1.0.0-RC1"
lazy val shardcakeVer  = "2.0.5"

lazy val catsVer      = "2.9.0"
lazy val sttpVer      = "3.8.3"
lazy val quicklensVer = "1.9.0"
lazy val upickleVer   = "2.0.0"
lazy val pprintVer    = "0.8.1"
lazy val osLibVer     = "0.8.1"

lazy val slf4jVer    = "1.7.36"
lazy val munitVer    = "1.0.0-M7"
lazy val hoconVer    = "1.4.2"
lazy val jodaTimeVer = "2.12.1"
lazy val minioVer    = "8.4.6"
lazy val quillVer    = "4.6.0"
lazy val postgresVer = "42.5.1"

lazy val flinkVer = "1.16.0"

lazy val commonSettings = Seq(
  ThisBuild / organization := "com.github.potamois",
  ThisBuild / version      := "0.1.0-SNAPSHOT",
  ThisBuild / developers := List(
    Developer(
      id = "Al-assad",
      name = "Linying Assad",
      email = "assad.dev@outlook.com",
      url = new URL("https://github.com/Al-assad")
    )),
  ThisBuild / scalaVersion := scala3,
  ThisBuild / javacOptions ++= Seq("-source", javaVer, "-target", javaVer),
  libraryDependencies ++= Seq(
    "dev.zio"       %% "zio"                 % zioVer,
    "dev.zio"       %% "zio-config"          % zioConfig,
    "dev.zio"       %% "zio-config-magnolia" % zioConfig,
    "dev.zio"       %% "zio-config-typesafe" % zioConfig,
    "com.typesafe"   % "config"              % hoconVer,
    "dev.zio"       %% "zio-test"            % zioVer   % Test,
    "dev.zio"       %% "zio-test-sbt"        % zioVer   % Test,
    "org.scalameta" %% "munit"               % munitVer % Test,
  ),
  testFrameworks := Seq(TestFramework("zio.test.sbt.ZTestFramework"), TestFramework("munit.Framework")),
)

lazy val root = (project in file("."))
  .settings(name := "potamoi")
  .aggregate(potaLogger, potaCore, potaFlink, potaServer)

lazy val potaLogger = (project in file("potamoi-logger"))
  .settings(commonSettings)
  .settings(
    name := "potamoi-logger",
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api"         % slf4jVer,
      "dev.zio"  %% "zio-logging"       % zioLoggingVer,
      "dev.zio"  %% "zio-logging-slf4j" % zioLoggingVer,
    )
  )

lazy val potaCore = (project in file("potamoi-core"))
  .dependsOn(potaLogger)
  .settings(commonSettings)
  .settings(
    name := "potamoi-core",
    libraryDependencies ++= Seq(
      "dev.zio"                       %% "zio-concurrent"          % zioVer,
      "dev.zio"                       %% "zio-json"                % zioJsonVer,
      "dev.zio"                       %% "zio-http"                % zioHttpVer,
      "dev.zio"                       %% "zio-direct"              % zioDirect exclude ("com.lihaoyi", "geny_2.13"),
      "org.typelevel"                 %% "cats-core"               % catsVer,
      "com.lihaoyi"                   %% "upickle"                 % upickleVer,
      "com.lihaoyi"                   %% "pprint"                  % pprintVer,
      "com.lihaoyi"                   %% "os-lib"                  % osLibVer,
      "com.softwaremill.quicklens"    %% "quicklens"               % quicklensVer,
      "com.softwaremill.sttp.client3" %% "core"                    % sttpVer,
      "com.softwaremill.sttp.client3" %% "zio"                     % sttpVer,
      "com.softwaremill.sttp.client3" %% "zio-json"                % sttpVer,
      "com.softwaremill.sttp.client3" %% "slf4j-backend"           % sttpVer,
      "joda-time"                      % "joda-time"               % jodaTimeVer,
      "com.coralogix"                 %% "zio-k8s-client"          % zioK8sVer,
      "io.getquill"                   %% "quill-jdbc-zio"          % quillVer exclude ("com.lihaoyi", "geny_2.13"),
      "org.postgresql"                 % "postgresql"              % postgresVer,
      "io.minio"                       % "minio"                   % minioVer,
      "com.devsisters"                %% "shardcake-manager"       % shardcakeVer,
      "com.devsisters"                %% "shardcake-entities"      % shardcakeVer,
      "com.devsisters"                %% "shardcake-protocol-grpc" % shardcakeVer,
    )
  )

lazy val potaFlink = (project in file("potamoi-flink"))
  .dependsOn(potaCore)
  .settings(commonSettings)
  .settings(
    name := "potamoi-flink",
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-clients"    % flinkVer,
      "org.apache.flink" % "flink-kubernetes" % flinkVer
    )
  )

lazy val potaServer = (project in file("potamoi-server"))
  .dependsOn(potaCore)
  .settings(commonSettings)
  .settings(
    name := "potamoi-server",
    libraryDependencies ++= Seq(
    )
  )
