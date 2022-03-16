lazy val scala = "2.12.15"
lazy val akkaVersion = "2.6.18"
lazy val akkaHttpVersion = "10.2.9"
lazy val flinkVersion = 14

lazy val commonSettings = Seq(
  organization := "com.github.potamois",
  version := "0.1.0",
  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html")),

  scalaVersion := scala,
  Compile / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint"),
  Compile / javacOptions ++= Seq("-encoding:UTF-8", "-Xlint:unchecked", "-Xlint:deprecation"),
  run / fork := true,
  Global / cancelable := false,
  Test / parallelExecution := false,

  libraryDependencies ++= Seq(
    "ch.qos.logback" % "logback-classic" % "1.2.11",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
    "org.scalatest" %% "scalatest" % "3.2.11" % Test,

    "io.spray" %% "spray-json" % "1.3.6",
    "com.github.nscala-time" %% "nscala-time" % "2.30.0",
    "org.apache.commons" % "commons-text" % "1.9"
  )
)


// root module
lazy val root = Project(id = "potamoi", base = file("."))
  .aggregate(commons, flinkGateway)
  .settings(commonSettings)

// commons module
lazy val commons = Project(id = "potamoi-commons", base = file("potamoi-commons"))
  .settings(
    commonSettings,
    libraryDependencies ++= akkaDeps // todo separated akka-tools to another sub-project
  )

// flink gateway module
lazy val flinkGateway = Project(id = "potamoi-flink-gateway", base = file("potamoi-flink-gateway"))
  .dependsOn(commons)
  .settings(
    commonSettings,
    libraryDependencies ++= akkaDeps ++ flinkDeps(flinkVersion) ++ tmpDeps,
  )
  .enablePlugins(JavaAppPackaging)


// akka dependencies
lazy val akkaDeps = Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-serialization-jackson" % akkaVersion,

  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-stream-typed" % akkaVersion,

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
    "org.apache.flink" %% "flink-clients")
    .map(_ % flinkVersionMap(majorVer))
  else Seq(
    "org.apache.flink" %% "flink-table-planner-blink",
    "org.apache.flink" %% "flink-clients")
    .map(_ % flinkVersionMap(majorVer) exclude("com.typesafe.akka", s"akka-protobuf_$scalaMajorVer"))


// todo remove in the future
val tmpDeps = Seq(
  "com.github.knaufk" % "flink-faker" % "0.4.1"
)


lazy val scalaMajorVer = scala.split("\\.").take(2).mkString(".")

