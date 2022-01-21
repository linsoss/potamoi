
lazy val commonSettings = Seq(
  organization := "com.github.potamois",
  version := "0.1.0",
  scalaVersion := "2.12.15"
)
javacOptions ++= Seq("-encoding", "UTF-8")

lazy val akkaVersion = "2.6.18"
lazy val akkaHttpVersion = "10.2.7"


// modules
lazy val root = Project(id = "potamoi", base = file("."))
  .settings(commonSettings)

lazy val commons = Project(id = "potamoi-commons", base = file("potamoi-commons"))
  .settings(commonSettings,
    libraryDependencies := Seq(
      "ch.qos.logback" % "logback-classic" % "1.2.10",
      "org.scalatest" %% "scalatest" % "3.2.9" % Test
    ))

lazy val flinkGateway = Project(id = "potamoi-flink-gateway", base = file("potamoi-flink-gateway"))
  .dependsOn(commons)
  .settings(commonSettings,
    libraryDependencies := akkaDeps ++ flinkDeps)


// akka dependencies
lazy val akkaDeps = Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test
)

// flink dependencies
lazy val flinkVersion = "1.14.3"

lazy val flinkDeps = Seq(
  "org.apache.flink" %% "flink-table-planner",
  "org.apache.flink" %% "flink-clients",
).map(_ % flinkVersion)


