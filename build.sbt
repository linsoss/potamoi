lazy val scala = "2.12.15"

lazy val akkaVersion = "2.6.18"
lazy val akkaHttpVersion = "10.2.9"
lazy val akkaKryoVersion = "2.4.3"
lazy val flinkVersion = 14

lazy val commonSettings = Seq(
  organization := "com.github.potamois",
  version := "0.1.0",
  maintainer := "Al-assad <assad.dev@outlook.com>",
  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html")),

  scalaVersion := scala,
  Compile / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint"),
  Compile / javacOptions ++= Seq("-Xlint:unchecked", "-Xlint:deprecation"),
  run / fork := true,
  Global / cancelable := false,
  Test / parallelExecution := false,

  libraryDependencies ++= Seq(
    "ch.qos.logback" % "logback-classic" % "1.2.11",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
    "org.scalatest" %% "scalatest" % "3.2.11" % Test
  )
)

// scala major version like "2.13"
lazy val scalaMajorVer = scala.split("\\.").take(2).mkString(".")

// root module
lazy val root = Project(id = "potamoi", base = file("."))
  .aggregate(commons, akkaToolkit, flinkGateway)
  .settings(commonSettings)

// potamoi commons module
// todo support s3 in commons module temporarily
lazy val commons = Project(id = "potamoi-commons", base = file("potamoi-commons"))
  .settings(
    commonSettings,
    libraryDependencies ++= deps(hoconConfigDep, apacheCommonsTextDep, nscalaTimeDep, minioDep),
  )

// potamoi akka-toolkit module
lazy val akkaToolkit = Project(id = "potamoi-akka-kit", base = file("potamoi-akka-kit"))
  .dependsOn(commons)
  .settings(
    commonSettings,
    libraryDependencies ++= deps(offerAkkaDeps(Provided))
  )

// potamoi flink-gateway module
lazy val flinkGateway = Project(id = "potamoi-flink-gateway", base = file("potamoi-flink-gateway"))
  .dependsOn(commons, akkaToolkit)
  .settings(
    commonSettings,
    libraryDependencies ++= deps(akkaDeps, flinkDeps(flinkVersion), sprayDep)
  )
  .enablePlugins(JavaAppPackaging)


// akka dependencies
lazy val akkaDeps = offerAkkaDeps(Compile)

def offerAkkaDeps(scope: Configuration) = Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion % scope,
  "com.typesafe.akka" %% "akka-cluster-typed" % akkaVersion % scope,
  "com.typesafe.akka" %% "akka-cluster-sharding-typed" % akkaVersion % scope,
  "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion % scope,
  "io.altoo" %% "akka-kryo-serialization-typed" % akkaKryoVersion % scope,

  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion % scope,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion % scope,
  "com.typesafe.akka" %% "akka-stream-typed" % akkaVersion % scope,

  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % Test,
  "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % Test
)

// flink dependencies
lazy val flinkVersionMap = Map(
  14 -> "1.14.3",
  13 -> "1.13.5",
  12 -> "1.12.7",
  11 -> "1.11.6"
)

def flinkDeps(majorVer: Int = 14) =
  if (majorVer >= 14) Seq(
    "org.apache.flink" %% "flink-table-planner",
    "org.apache.flink" %% "flink-clients",
    "org.apache.flink" %% "flink-kubernetes")
    .map(_ % flinkVersionMap(majorVer))
  else Seq(
    "org.apache.flink" %% "flink-table-planner-blink",
    "org.apache.flink" %% "flink-clients",
    "org.apache.flink" %% "flink-kubernetes")
    .map(_ % flinkVersionMap(majorVer) exclude("com.typesafe.akka", s"akka-protobuf_$scalaMajorVer"))

// other dependencies
lazy val hoconConfigDep = "com.typesafe" % "config" % "1.4.2"
lazy val sprayDep = "io.spray" %% "spray-json" % "1.3.6"
lazy val nscalaTimeDep = "com.github.nscala-time" %% "nscala-time" % "2.30.0"
lazy val apacheCommonsTextDep = "org.apache.commons" % "commons-text" % "1.9"
lazy val minioDep = "io.minio" % "minio" % "8.3.7"


def deps(moduleIds: Any*): Seq[ModuleID] = moduleIds.flatMap {
  case id: ModuleID => Seq(id)
  case ids: Seq[ModuleID@unchecked] => ids
  case _ => Seq()
}
