ThisBuild / scalaVersion := "3.7.4"

lazy val sparkVersion = "4.0.1"
lazy val deltaVersion = "4.0.0"
lazy val scalaReflectVersion = "2.13.17"
lazy val catsVersion = "2.13.0"
lazy val catsEffectVersion = "3.7.1"
lazy val catsRetryVersion = "4.0.0"
lazy val declineVersion = "2.6.1"
lazy val fs2Version = "3.14.0"
lazy val log4catsVersion = "2.8.0"
lazy val mongoVersion = "5.13.0"
lazy val testcontainersVersion = "2.0.5"
lazy val munitVersion = "1.3.6"
lazy val munitCatsEffectVersion = "2.2.1"
lazy val pureConfigVersion = "0.17.10"
lazy val circeVersion = "0.14.16"
lazy val typesafeConfigVersion = "1.4.9"
lazy val ironVersion = "3.3.2"
lazy val mongo4catsVersion = "0.7.18"
lazy val munitScalaCheckVersion = "1.3.1"
lazy val archUnitVersion = "1.5.1"
lazy val IntegrationTest = config("it") extend Test

lazy val analytics = (project in file("."))
  .enablePlugins(JacocoItPlugin)
  .configs(IntegrationTest)
  .settings(inConfig(IntegrationTest)(Defaults.testSettings))
  .settings(
    name := "hiring-analytics",
    version := "0.1.0-SNAPSHOT",
    target := sys.props
      .get("hiring.test.buildRoot")
      .fold(baseDirectory.value / "target")(path => file(path) / "target"),
    publish / skip := true,
    Compile / run / fork := true,
    Test / fork := true,
    Test / jacocoReportSettings := JacocoReportSettings()
      .withFormats(JacocoReportFormats.ScalaHTML, JacocoReportFormats.XML),
    IntegrationTest / jacocoMergedReportSettings := JacocoReportSettings()
      .withThresholds(JacocoThresholds(line = 85))
      .withFormats(JacocoReportFormats.ScalaHTML, JacocoReportFormats.XML),
    Test / unmanagedSourceDirectories += baseDirectory.value.getParentFile / "test-support" / "src" / "main" / "scala",
    Test / parallelExecution := false,
    Test / javaOptions += "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    IntegrationTest / scalaSource := baseDirectory.value / "src" / "it" / "scala",
    IntegrationTest / parallelExecution := false,
    // sbt-jacoco hands the exec-file location to the JVM through javaOptions, so only a forked JVM records data.
    IntegrationTest / fork := true,
    // Defaults.testSettings above redefines fullClasspath after the plugin; restore the same definition with the
    // jacoco-instrumented classes first (as Test has), otherwise integration tests run uninstrumented classes.
    IntegrationTest / fullClasspath := {
      val instrumentedDirectory = (IntegrationTest / jacocoInstrumentedDirectory).value
      val instrumented = (Test / fullClasspath).value.filter(_.data == instrumentedDirectory)
      instrumented ++ Classpaths
        .concatDistinct(IntegrationTest / exportedProducts, IntegrationTest / dependencyClasspath)
        .value
    },
    IntegrationTest / javaOptions += "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    scalacOptions ++= Seq(
      "-encoding",
      "utf-8",
      "-release:17",
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Wunused:all",
      "-Wvalue-discard",
      "-Werror"
    ),
    dependencyOverrides += "org.scala-lang" % "scala-reflect" % scalaReflectVersion,
    libraryDependencies ++= Seq(
      ("org.apache.spark" %% "spark-sql" % sparkVersion).cross(CrossVersion.for3Use2_13),
      ("org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion).cross(CrossVersion.for3Use2_13),
      ("io.delta" %% "delta-spark" % deltaVersion).cross(CrossVersion.for3Use2_13),
      "org.typelevel" %% "cats-core" % catsVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "com.github.cb372" %% "cats-retry" % catsRetryVersion,
      "com.monovore" %% "decline-effect" % declineVersion,
      "com.github.pureconfig" %% "pureconfig-core" % pureConfigVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion % Test,
      "com.typesafe" % "config" % typesafeConfigVersion,
      "io.github.iltotore" %% "iron" % ironVersion,
      "io.github.iltotore" %% "iron-pureconfig" % ironVersion,
      "co.fs2" %% "fs2-core" % fs2Version,
      "co.fs2" %% "fs2-io" % fs2Version,
      "org.typelevel" %% "log4cats-slf4j" % log4catsVersion,
      "io.github.kirill5k" %% "mongo4cats-core" % mongo4catsVersion,
      "io.github.kirill5k" %% "mongo4cats-circe" % mongo4catsVersion,
      "org.mongodb" % "mongodb-driver-reactivestreams" % mongoVersion,
      "org.mongodb" % "mongodb-driver-sync" % mongoVersion % Test,
      "co.fs2" %% "fs2-reactive-streams" % fs2Version,
      "org.testcontainers" % "testcontainers" % testcontainersVersion % Test,
      "org.testcontainers" % "testcontainers-kafka" % testcontainersVersion % Test,
      "org.scalameta" %% "munit" % munitVersion % Test,
      "org.typelevel" %% "munit-cats-effect" % munitCatsEffectVersion % Test,
      "org.typelevel" %% "cats-effect-testkit" % catsEffectVersion % Test,
      "org.scalameta" %% "munit-scalacheck" % munitScalaCheckVersion % Test,
      "com.tngtech.archunit" % "archunit" % archUnitVersion % Test
    )
  )
