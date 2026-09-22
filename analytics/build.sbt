ThisBuild / scalaVersion := "2.13.16"

lazy val sparkVersion = "4.0.1"
lazy val deltaVersion = "4.0.0"
lazy val munitVersion = "1.3.6"

lazy val analytics = (project in file("."))
  .settings(
    name := "hiring-analytics",
    version := "0.1.0-SNAPSHOT",
    publish / skip := true,
    Compile / run / fork := true,
    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions += "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Werror"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
      "io.delta" %% "delta-spark" % deltaVersion,
      "org.scalameta" %% "munit" % munitVersion % Test
    )
  )
