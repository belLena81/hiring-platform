ThisBuild / scalaVersion := "3.9.0"

lazy val sparkVersion = "4.0.1"
lazy val deltaVersion = "4.0.0"
lazy val munitVersion = "1.3.6"
lazy val munitCatsEffectVersion = "2.2.1"
lazy val IntegrationTest = config("it") extend Test

lazy val analytics = (project in file("."))
  .configs(IntegrationTest)
  .settings(inConfig(IntegrationTest)(Defaults.testSettings))
  .settings(
    name := "hiring-analytics",
    version := "0.1.0-SNAPSHOT",
    publish / skip := true,
    Compile / run / fork := true,
    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions += "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    IntegrationTest / scalaSource := baseDirectory.value / "src" / "it" / "scala",
    IntegrationTest / parallelExecution := false,
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Werror"),
    csrSameVersions := Nil,
    dependencyOverrides += "org.scala-lang" % "scala-reflect" % "2.13.16",
    libraryDependencies ++= Seq(
      ("org.apache.spark" %% "spark-sql" % sparkVersion).cross(CrossVersion.for3Use2_13),
      ("org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion).cross(CrossVersion.for3Use2_13),
      ("io.delta" %% "delta-spark" % deltaVersion).cross(CrossVersion.for3Use2_13),
      "org.typelevel" %% "cats-core" % "2.13.0",
      "org.typelevel" %% "cats-effect" % "3.7.1",
      "co.fs2" %% "fs2-core" % "3.14.0",
      "org.typelevel" %% "log4cats-slf4j" % "2.8.0",
      "org.mongodb" % "mongodb-driver-sync" % "5.12.0",
      "org.testcontainers" % "testcontainers" % "2.0.5" % Test,
      "org.scalameta" %% "munit" % munitVersion % Test,
      "org.typelevel" %% "munit-cats-effect" % munitCatsEffectVersion % Test
    )
  )
