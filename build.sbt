lazy val Scala = "3.2.0"

lazy val ScalaLoggingVer = "3.9.5"
lazy val LogbackVer = "1.2.11"
lazy val HoconVer = "1.4.2"

lazy val ZIOVer = "2.0.1"
lazy val ZIOConfigVer = "3.0.2"
lazy val ZIOHttpVer = "2.0.0-RC11"
lazy val ZIOJsonVer = "0.3.0-RC11"

lazy val TapirVer = "1.1.0"
lazy val QuillVer = "4.4.1"
lazy val HikariVer = "3.4.5"
lazy val PostgresDriverVer = "42.4.2"
lazy val JwtCoreVer = "9.1.1"
lazy val MUnitVer = "0.7.29"
lazy val MunitZIOVer = "0.1.1"

lazy val commonSettings = Seq(
  organization := "com.github.potamois",
  version := "0.1",
  scalaVersion := Scala,
  Compile / javacOptions ++= Seq("-source", "11", "-target", "11"),
  Compile / scalaSource := baseDirectory.value / "src",
  Compile / javaSource := baseDirectory.value / "src",
  Compile / resourceDirectory := baseDirectory.value / "resources",
  Test / scalaSource := baseDirectory.value / "test" / "src",
  Test / javaSource := baseDirectory.value / "test" / "src",
  Test / resourceDirectory := baseDirectory.value / "test" / "resources",
  run / fork := true,
  Global / cancelable := false,
  Test / parallelExecution := false
)


lazy val Root = Project(id = "potamoi", base = file("."))
  .aggregate(PotamoiCommon, PotamoiCore)

lazy val PotamoiCommon = Project(id = "potamoi-common", base = file("potamoi-common"))
  .settings(commonSettings: _*)
  .settings(
    name := "potamoi-common",
    libraryDependencies ++= Seq(
      "com.typesafe.scala-logging" %% "scala-logging" % ScalaLoggingVer,
      "ch.qos.logback" % "logback-classic" % LogbackVer,
      "com.typesafe" % "config" % HoconVer
    )
  )

lazy val PotamoiCore = Project(id = "potamoi-core", base = file("potamoi-core"))
  .settings(commonSettings: _*)
  .settings(
    name := "potamoi-core",
    libraryDependencies ++=
    ZIODep ++
    ZIOTestKitDep.map(_ % Test) ++
    ZIOConfigDep ++
    ZIOJsonDep ++
    ZIOHttpDep ++
    TapirDep ++
    QuillDep
  )
  .dependsOn(PotamoiCommon)


lazy val ZIODep = Seq(
  "zio",
  "zio-concurrent",
  "zio-streams",
  "zio-logging"
).map("dev.zio" %% _ % ZIOVer)

lazy val ZIOTestKitDep = Seq(
  "zio-test",
  "zio-test-sbt",
  "zio-test-magnolia",
).map("dev.zio" %% _ % ZIOVer) ++ MunitZIODep

lazy val MunitZIODep = Seq(
  "org.scalameta" %% "munit" % MUnitVer,
  "com.github.poslegm" %% "munit-zio" % MunitZIOVer
)

lazy val ZIOConfigDep = Seq(
  "zio-config",
  "zio-config-magnolia",
  "zio-config-typesafe"
).map("dev.zio" %% _ % ZIOConfigVer)

lazy val ZIOHttpDep = Seq(
  "io.d11" %% "zhttp" % ZIOHttpVer,
  "com.github.jwt-scala" %% "jwt-core" % JwtCoreVer
)

lazy val ZIOJsonDep = Seq(
  "dev.zio" %% "zio-json" % ZIOJsonVer
)

lazy val QuillDep = Seq(
  "io.getquill" %% "quill-jdbc-zio" % QuillVer,
  "io.getquill" %% "quill-jdbc" % QuillVer,
  "org.postgresql" % "postgresql" % PostgresDriverVer
)

lazy val TapirDep = Seq(
  "tapir-core",
  "tapir-sttp-client",
  "tapir-swagger-ui-bundle",
  "tapir-zio-http-server"
).map("com.softwaremill.sttp.tapir" %% _ % TapirVer)