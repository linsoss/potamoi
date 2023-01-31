lazy val scala3   = "3.2.1"
lazy val scala213 = "2.13.10"
lazy val scala212 = "2.12.17"
lazy val javaVer  = "17"

// Keep ZIO at 2.0.2, because the ZLayer macros of ZIO 2.0.3+ has some problems
// with Scala3 compatibility, especially with sttp-client libs.
// see: https://github.com/zio/zio/issues/7506
lazy val zioVer        = "2.0.2"
lazy val zioLoggingVer = "2.1.5"
lazy val zioConfig     = "3.0.2"
lazy val zioJsonVer    = "0.4.2"
lazy val zioHttpVer    = "0.0.3"
lazy val zioK8sVer     = "2.0.1"
lazy val zioDirectVer  = "1.0.0-RC1"
lazy val zioSchemaVer  = "0.4.0"
lazy val zioCacheVer   = "0.2.1"
lazy val shardcakeVer  = "2.0.5"

// Keep Akka at 2.6.20 which is the last version in the apache license and
// replace it with Pekko in the future.
lazy val akkaVer     = "2.6.20"
lazy val akkaKryoVer = "2.5.0"

lazy val scalaTestVer        = "3.2.15"
lazy val scalaJava8CompatVer = "1.0.2"
lazy val catsVer             = "2.9.0"
lazy val sttpVer             = "3.8.5"
lazy val quicklensVer        = "1.9.0"
lazy val upickleVer          = "2.0.0"
lazy val pprintVer           = "0.8.1"
lazy val osLibVer            = "0.8.1"
lazy val circeVer            = "0.14.3"

lazy val slf4jVer       = "1.7.36"
lazy val munitVer       = "1.0.0-M7"
lazy val jodaTimeVer    = "2.12.2"
lazy val tikaVer        = "2.6.0"
lazy val commonCodecVer = "1.15"
lazy val minioVer       = "8.5.1"
lazy val quillVer       = "4.6.0"
lazy val postgresVer    = "42.5.1"

lazy val flinkVer    = flink116Ver
lazy val flink116Ver = "1.16.0"
lazy val flink115Ver = "1.15.3"

lazy val commonSettings = Seq(
  ThisBuild / organization := "com.github.potamois",
  ThisBuild / version      := "0.1.0-SNAPSHOT",
  ThisBuild / developers   := List(
    Developer(
      id = "Al-assad",
      name = "Linying Assad",
      email = "assad.dev@outlook.com",
      url = new URL("https://github.com/Al-assad")
    )),
  ThisBuild / scalaVersion := scala3,
  ThisBuild / scalacOptions ++= Seq("-Xmax-inlines", "64"),
  ThisBuild / javacOptions ++= Seq("-source", javaVer, "-target", javaVer) ++ jdk17opens.flatMap(Seq("--add-opens", _)),
  libraryDependencies ++= Seq(
    "dev.zio"       %% "zio"                   % zioVer,
    "dev.zio"       %% "zio-concurrent"        % zioVer,
    "dev.zio"       %% "zio-config"            % zioConfig,
    "dev.zio"       %% "zio-config-magnolia"   % zioConfig,
    "dev.zio"       %% "zio-config-typesafe"   % zioConfig,
    "org.slf4j"      % "slf4j-api"             % slf4jVer,
    "dev.zio"       %% "zio-logging"           % zioLoggingVer,
    "dev.zio"       %% "zio-logging-slf4j"     % zioLoggingVer,
    "dev.zio"       %% "zio-json"              % zioJsonVer,
    "dev.zio"       %% "zio-schema"            % zioSchemaVer,
    "dev.zio"       %% "zio-schema-derivation" % zioSchemaVer,
    "dev.zio"       %% "zio-schema-json"       % zioSchemaVer,
    "dev.zio"       %% "zio-schema-protobuf"   % zioSchemaVer,
    "org.typelevel" %% "cats-core"             % catsVer,
    "io.circe"      %% "circe-core"            % circeVer,
    "io.circe"      %% "circe-parser"          % circeVer,
    "dev.zio"       %% "zio-test"              % zioVer       % Test,
    "dev.zio"       %% "zio-test-sbt"          % zioVer       % Test,
    "org.scalatest" %% "scalatest"             % scalaTestVer % Test
  ),
  testFrameworks           := Seq(TestFramework("zio.test.sbt.ZTestFramework")),
)

lazy val jdk17opens = Seq(
  "java.base/java.lang=ALL-UNNAMED",
  "java.base/java.lang.invoke=ALL-UNNAMED",
  "java.base/java.lang.reflect=ALL-UNNAMED",
  "java.base/java.io=ALL-UNNAMED",
  "java.base/java.net=ALL-UNNAMED",
  "java.base/java.nio=ALL-UNNAMED",
  "java.base/java.util=ALL-UNNAMED",
  "java.base/java.util.concurrent=ALL-UNNAMED",
  "java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "java.base/java.math=ALL-UNNAMED",
  "java.base/sun.nio.ch=ALL-UNNAMED",
  "java.base/sun.nio.cs=ALL-UNNAMED",
  "java.base/sun.security.action=ALL-UNNAMED",
  "java.base/sun.util.calendar=ALL-UNNAMED",
  "java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(name := "potamoi")
  .aggregate(
    potaCommon,
    potaFilesystem,
    potaKubernetes,
    potaServer,
    potaFlink,
//    potaFlinkShare,
//    potaFlinkInterpreterBase,
//    potaFlinkInterpreterV116,
//    potaFlinkInterpreterV115
  )

lazy val potaCommon = (project in file("potamoi-common"))
  .settings(commonSettings)
  .settings(
    name := "potamoi-common",
    libraryDependencies ++= Seq(
      akkaDep("com.typesafe.akka" %% "akka-actor-typed" % akkaVer),
      "dev.zio"                       %% "zio-http"       % zioHttpVer exclude ("dev.zio", "zio_3") exclude ("dev.zio", "zio-streams_3"),
      "dev.zio"                       %% "zio-direct"     % zioDirectVer exclude ("com.lihaoyi", "geny_2.13"),
      "dev.zio"                       %% "zio-cache"      % zioCacheVer,
      "com.lihaoyi"                   %% "upickle"        % upickleVer,
      "com.lihaoyi"                   %% "pprint"         % pprintVer,
      "com.softwaremill.quicklens"    %% "quicklens"      % quicklensVer,
      "com.softwaremill.sttp.client3" %% "core"           % sttpVer,
      "com.softwaremill.sttp.client3" %% "zio-json"       % sttpVer,
      "com.softwaremill.sttp.client3" %% "zio"            % sttpVer,
      "com.softwaremill.sttp.client3" %% "slf4j-backend"  % sttpVer,
      "joda-time"                      % "joda-time"      % jodaTimeVer,
      "org.apache.tika"                % "tika-core"      % tikaVer exclude ("org.slf4j", "slf4j-api"),
      "commons-codec"                  % "commons-codec"  % commonCodecVer,
      "io.getquill"                   %% "quill-jdbc-zio" % quillVer exclude ("com.lihaoyi", "geny_2.13"),
      "org.postgresql"                 % "postgresql"     % postgresVer,
    )
  )

lazy val potaKubernetes = (project in file("potamoi-kubernetes"))
  .dependsOn(potaCommon)
  .settings(commonSettings)
  .settings(
    name := "potamoi-kubernetes",
    libraryDependencies ++= Seq(
      "com.coralogix" %% "zio-k8s-client" % zioK8sVer,
    )
  )

lazy val potaFilesystem = (project in file("potamoi-filesystem"))
  .dependsOn(potaCommon)
  .settings(commonSettings)
  .settings(
    name := "potamoi-filesystem",
    libraryDependencies ++= Seq(
      "com.lihaoyi" %% "os-lib" % osLibVer,
      "io.minio"     % "minio"  % minioVer
    )
  )

lazy val potaCluster = (project in file("potamoi-cluster"))
  .dependsOn(potaCommon, potaKubernetes)
  .settings(commonSettings)
  .settings(
    name := "potamoi-cluster",
    libraryDependencies ++= Seq(
      "com.devsisters"         %% "shardcake-manager"       % shardcakeVer,
      "com.devsisters"         %% "shardcake-entities"      % shardcakeVer,
      "com.devsisters"         %% "shardcake-protocol-grpc" % shardcakeVer,
      "com.devsisters"         %% "shardcake-storage-redis" % shardcakeVer,
      "com.devsisters"         %% "shardcake-health-k8s"    % shardcakeVer,
      akkaDep("com.typesafe.akka" %% "akka-actor-typed"            % akkaVer),
      akkaDep("com.typesafe.akka" %% "akka-cluster-typed"          % akkaVer),
      akkaDep("com.typesafe.akka" %% "akka-cluster-sharding-typed" % akkaVer),
      "io.altoo"               %% "akka-kryo-serialization" % akkaKryoVer exclude ("com.typesafe.akka", "akka-actor_2.13"),
      "org.scala-lang.modules" %% "scala-java8-compat"      % scalaJava8CompatVer
    )
  )

def akkaDep(moduleId: ModuleID) = moduleId cross CrossVersion.for3Use2_13 exclude ("org.scala-lang.modules", "scala-java8-compat_2.13")

lazy val potaServer = (project in file("potamoi-server"))
  .dependsOn(potaCommon, potaKubernetes, potaFilesystem)
  .settings(commonSettings)
  .settings(
    name := "potamoi-server",
    libraryDependencies ++= Seq(
    )
  )

// todo remove
//lazy val potaFlinkShare = (project in file("potamoi-flink-share"))
//  .dependsOn(potaCommon, potaFilesystem, potaKubernetes, potaCluster)
//  .settings(commonSettings)
//  .settings(
//    name := "potamoi-flink-share",
//    libraryDependencies ++= Seq(
//      "org.apache.flink" % "flink-table-api-java" % flinkVer % Provided
//    ).map(flinkLibExcludes)
//  )

lazy val potaFlink = (project in file("potamoi-flink"))
  .dependsOn(potaCommon, potaKubernetes, potaFilesystem, potaCluster)
  .settings(commonSettings)
  .settings(
    name := "potamoi-flink",
    libraryDependencies ++= flinkDeps(flinkVer).map(_ % Optional).map(flinkLibExcludes)
  )

//lazy val potaFlinkInterpreterBase = (project in file("potamoi-flink-interpreter/flink-base"))
//  .dependsOn(potaCommon, potaFilesystem, potaCluster, potaFlinkShare)
//  .settings(commonSettings)
//  .settings(
//    name := "potamoi-flink-interpreter-base",
//    libraryDependencies ++= flinkInterpreterDeps(flinkVer).map(_ % Provided).map(flinkLibExcludes) // todo
//  )
//
//lazy val potaFlinkInterpreterV116 = (project in file("potamoi-flink-interpreter/flink-v116"))
//  .dependsOn(potaFlinkInterpreterBase)
//  .settings(commonSettings)
//  .settings(
//    name := "potamoi-flink-interpreter-v116",
//    libraryDependencies ++= flinkInterpreterDeps(flink116Ver).map(flinkLibExcludes) // todo
//  )
//
//lazy val potaFlinkInterpreterV115 = (project in file("potamoi-flink-interpreter/flink-v115"))
//  .dependsOn(potaFlinkInterpreterBase)
//  .settings(commonSettings)
//  .settings(
//    name := "potamoi-flink-interpreter-v115",
//    libraryDependencies ++= flinkInterpreterDeps(flink115Ver).map(flinkLibExcludes) // todo
//  )

def flinkDeps(version: String) = Seq(
  "org.apache.flink"   % "flink-clients"              % version,
  "org.apache.flink"   % "flink-kubernetes"           % version,
  "org.apache.flink"   % "flink-table-planner-loader" % version,
  "org.apache.flink"   % "flink-table-runtime"        % version,
  "org.apache.flink"   % "flink-json"                 % version,
  "org.apache.flink"   % "flink-sql-parser"           % version,
  "org.apache.calcite" % "calcite-linq4j"             % "1.26.0" // Keep consistent with flink-sql-parser
)

def flinkInterpreterDeps(version: String) = Seq(
  "org.apache.flink"   % "flink-clients"              % version,
  "org.apache.flink"   % "flink-table-planner-loader" % version,
  "org.apache.flink"   % "flink-table-runtime"        % version,
  "org.apache.flink"   % "flink-json"                 % version,
  "org.apache.flink"   % "flink-sql-parser"           % version,
  "org.apache.calcite" % "calcite-linq4j"             % "1.26.0" // Keep consistent with flink-sql-parser
)

def flinkLibExcludes(moduleId: ModuleID) = moduleId
  .exclude("org.slf4j", "slf4j-api")
  .exclude("commons-logging", "commons-logging")
